"""Security contract for the literal unattended environment boundary."""

from __future__ import annotations

import json
import os
import shutil
import stat
import subprocess
import sys
import unittest
from collections.abc import Mapping, MutableMapping
from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

from stock_monitor import unattended
from stock_monitor.config import load_settings
from stock_monitor.unattended import (
    APPROVED_KEYS,
    LiteralEnvironmentError,
    load_literal_environment,
    parse_exact_literal_assignments,
    read_bounded_descriptor,
)


ROOT = Path(__file__).resolve().parents[2]
LAUNCHER = ROOT / "scripts" / "run_monitor_unattended.sh"
EXPECTED_LAUNCHER = (
    b"#!/bin/sh\n"
    b"set -eu\n"
    b"set +x\n"
    b"umask 077\n"
    b"case $0 in\n"
    b"  /*) script_path=$0 ;;\n"
    b"  *) exit 2 ;;\n"
    b"esac\n"
    b"script_dir=${script_path%/*}\n"
    b'repo_root=$(CDPATH= cd -- "$script_dir/.." && pwd -P)\n'
    b"python=$repo_root/.venv/bin/python3\n"
    b'[ -x "$python" ] || exit 2\n'
    b'exec /usr/bin/env -i "$python" -I -m stock_monitor.unattended "$@"\n'
)


class LiteralEnvironmentTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def valid_text(
        self,
        *,
        key_id: str = "literal-key-id",
        secret_key: str = "literal-secret-key",
        user_agent: str = "Stock Monitor test operator@example.com",
        home: str | None = None,
    ) -> str:
        return (
            "# Literal values only.\n"
            f"APCA_API_KEY_ID={key_id}\n"
            f"APCA_API_SECRET_KEY={secret_key}\n"
            f"SEC_USER_AGENT={user_agent}\n"
            f"STOCK_MONITOR_HOME={home or self.root / 'state'}\n"
        )

    def private_file(
        self,
        payload: str | bytes,
        *,
        name: str = "private.env",
        mode: int = 0o600,
    ) -> Path:
        path = self.root / name
        encoded = payload.encode("utf-8") if isinstance(payload, str) else payload
        path.write_bytes(encoded)
        path.chmod(mode)
        return path

    def test_command_substitution_and_backticks_remain_literal(self) -> None:
        marker = self.root / "marker"
        path = self.private_file(
            self.valid_text(key_id=f"$(touch {marker})`touch {marker}`")
        )

        values = load_literal_environment(path)

        self.assertEqual(
            values["APCA_API_KEY_ID"],
            f"$(touch {marker})`touch {marker}`",
        )
        self.assertFalse(marker.exists())

    def test_exact_keys_are_loaded_and_equals_remains_in_value(self) -> None:
        path = self.private_file(self.valid_text(secret_key="left=middle=right"))

        values = load_literal_environment(path)

        self.assertEqual(values.keys(), APPROVED_KEYS)
        self.assertEqual(values["APCA_API_SECRET_KEY"], "left=middle=right")

    def test_relative_operator_home_is_rejected(self) -> None:
        path = self.private_file(
            self.valid_text(home="relative-operator-state")
        )

        with self.assertRaisesRegex(
            LiteralEnvironmentError,
            "private environment operator home must be absolute",
        ):
            load_literal_environment(path)

    def test_symlink_hardlink_permissive_and_non_regular_files_are_rejected(self) -> None:
        target = self.private_file(self.valid_text(), name="symlink-target.env")
        symlink = self.root / "symlink.env"
        symlink.symlink_to(target)

        hardlink_target = self.private_file(
            self.valid_text(),
            name="hardlink-target.env",
        )
        hardlink = self.root / "hardlink.env"
        os.link(hardlink_target, hardlink)

        permissive = self.private_file(
            self.valid_text(),
            name="permissive.env",
            mode=0o640,
        )
        directory = self.root / "directory.env"
        directory.mkdir(mode=0o700)

        for path in (symlink, hardlink, permissive, directory):
            with self.subTest(path=path), self.assertRaises(LiteralEnvironmentError):
                load_literal_environment(path)

    def test_fifo_is_rejected_quickly_without_traceback_or_canary(self) -> None:
        canary = "CANARY_FIFO_PATH_MUST_NOT_ESCAPE"
        fifo = self.root / canary
        os.mkfifo(fifo, mode=0o600)
        program = (
            "import sys\n"
            "from pathlib import Path\n"
            "from stock_monitor.unattended import (\n"
            "    LiteralEnvironmentError, load_literal_environment\n"
            ")\n"
            "try:\n"
            "    load_literal_environment(Path(sys.argv[1]))\n"
            "except LiteralEnvironmentError:\n"
            "    print('CONFIGURATION REQUIRED')\n"
            "    raise SystemExit(2)\n"
            "except Exception:\n"
            "    print('INTERNAL ERROR')\n"
            "    raise SystemExit(10)\n"
            "print('UNSAFE SUCCESS')\n"
        )
        process = subprocess.Popen(
            [sys.executable, "-W", "error", "-c", program, str(fifo)],
            cwd=self.root,
            env={"PYTHONPATH": str(ROOT / "src")},
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        try:
            stdout, stderr = process.communicate(timeout=1.0)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            self.fail("FIFO loading blocked before descriptor validation")

        combined = stdout + stderr
        self.assertEqual(process.returncode, 2)
        self.assertEqual(stdout, "CONFIGURATION REQUIRED\n")
        self.assertEqual(stderr, "")
        self.assertNotIn(canary, combined)
        self.assertNotIn("Traceback", combined)

    def test_exact_private_mode_matrix(self) -> None:
        for mode in (0o400, 0o600):
            with self.subTest(mode=oct(mode), expected="accepted"):
                path = self.private_file(
                    self.valid_text(),
                    name=f"accepted-{mode:o}.env",
                    mode=mode,
                )
                self.assertEqual(
                    load_literal_environment(path)["APCA_API_KEY_ID"],
                    "literal-key-id",
                )

        for mode in (0o000, 0o100, 0o200, 0o500, 0o700, 0o601, 0o610, 0o640):
            with self.subTest(mode=oct(mode), expected="rejected"):
                path = self.private_file(
                    self.valid_text(),
                    name=f"rejected-{mode:o}.env",
                    mode=mode,
                )
                with self.assertRaises(LiteralEnvironmentError):
                    load_literal_environment(path)

    def test_wrong_owner_is_rejected(self) -> None:
        path = self.private_file(self.valid_text())

        with patch.object(unattended.os, "geteuid", return_value=os.geteuid() + 1):
            with self.assertRaises(LiteralEnvironmentError):
                load_literal_environment(path)

    def test_invalid_assignment_shapes_are_rejected(self) -> None:
        valid = self.valid_text()
        cases = {
            "duplicate": valid + "APCA_API_KEY_ID=second\n",
            "unknown": valid + "HTTP_PROXY=http://canary.invalid\n",
            "malformed": valid + "not-an-assignment\n",
            "incomplete": "APCA_API_KEY_ID=only-one\n",
            "empty": valid.replace(
                "APCA_API_SECRET_KEY=literal-secret-key",
                "APCA_API_SECRET_KEY=",
            ),
        }
        for name, payload in cases.items():
            with self.subTest(case=name):
                path = self.private_file(payload, name=f"{name}.env")
                with self.assertRaises(LiteralEnvironmentError):
                    load_literal_environment(path)

    def test_oversized_invalid_utf8_and_control_values_are_rejected(self) -> None:
        cases = {
            "oversized": self.valid_text(secret_key="x" * 16384).encode("utf-8"),
            "invalid-utf8": b"\xff\xfe",
            "nul": self.valid_text(secret_key="before\x00after").encode("utf-8"),
            "control": self.valid_text(secret_key="before\x1fafter").encode("utf-8"),
            "delete-control": self.valid_text(secret_key="before\x7fafter").encode(
                "utf-8"
            ),
            "c1-control": self.valid_text(secret_key="before\x80after").encode("utf-8"),
        }
        for name, payload in cases.items():
            with self.subTest(case=name):
                path = self.private_file(payload, name=f"{name}.env")
                with self.assertRaises(LiteralEnvironmentError):
                    load_literal_environment(path)

    def test_every_non_lf_control_codepoint_is_rejected_in_comments(self) -> None:
        prefix = self.valid_text().encode("utf-8") + b"# comment"
        controls = (*range(0, 10), *range(11, 32), *range(127, 160))

        for codepoint in controls:
            with self.subTest(codepoint=codepoint):
                payload = prefix + chr(codepoint).encode("utf-8") + b"hidden\n"
                with self.assertRaises(LiteralEnvironmentError):
                    parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

    def test_non_printable_unicode_is_rejected_in_every_grammar_position(self) -> None:
        characters = (
            "\N{LINE SEPARATOR}",
            "\N{PARAGRAPH SEPARATOR}",
            "\N{ZERO WIDTH SPACE}",
            "\N{RIGHT-TO-LEFT OVERRIDE}",
            "\N{WORD JOINER}",
            "\N{ZERO WIDTH NO-BREAK SPACE}",
            "\N{SOFT HYPHEN}",
        )
        for character in characters:
            self.assertFalse(character.isprintable())
            cases = {
                "comment": (
                    self.valid_text() + f"# hidden{character}content\n"
                ).encode("utf-8"),
                "key": self.valid_text().replace(
                    "APCA_API_KEY_ID=",
                    f"APCA_API_KEY{character}_ID=",
                    1,
                ).encode("utf-8"),
                "value": self.valid_text(
                    secret_key=f"before{character}after"
                ).encode("utf-8"),
            }
            for position, payload in cases.items():
                with self.subTest(
                    codepoint=f"U+{ord(character):04X}",
                    position=position,
                ), self.assertRaises(LiteralEnvironmentError):
                    parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

    def test_controls_in_keys_and_around_separators_are_rejected(self) -> None:
        valid = self.valid_text().encode("utf-8")
        cases = (
            valid.replace(
                b"APCA_API_KEY_ID=",
                b"APCA_API_KEY\x00_ID=",
                1,
            ),
            valid.replace(
                b"APCA_API_KEY_ID=",
                b"APCA_API_KEY_ID\x1f=",
                1,
            ),
            valid.replace(
                b"APCA_API_KEY_ID=",
                b"APCA_API_KEY_ID=\x7f",
                1,
            ),
        )

        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(
                LiteralEnvironmentError
            ):
                parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

    def test_splitlines_controls_and_crlf_are_rejected(self) -> None:
        valid = self.valid_text().encode("utf-8")
        splitline_controls = (
            b"\x0b",
            b"\x0c",
            b"\x1c",
            b"\x1d",
            b"\x1e",
            b"\xc2\x85",
        )
        cases = (
            *(
                valid + b"# first" + control + b"# second\n"
                for control in splitline_controls
            ),
            valid.replace(b"\n", b"\r\n"),
            valid + b"# comment\rhidden\n",
            valid.replace(b"\n", "\N{LINE SEPARATOR}".encode("utf-8"), 1),
            valid.replace(b"\n", "\N{PARAGRAPH SEPARATOR}".encode("utf-8"), 1),
        )

        for payload in cases:
            with self.subTest(payload=payload), self.assertRaises(
                LiteralEnvironmentError
            ):
                parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

    def test_printable_utf8_in_comments_survives_payload_validation(self) -> None:
        payload = (
            "# opérateur\n".encode("utf-8")
            + self.valid_text().encode("utf-8")
        )

        values = parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

        self.assertEqual(values["APCA_API_KEY_ID"], "literal-key-id")

    def test_printable_utf8_in_values_remains_literal(self) -> None:
        printable = "café-東京-🙂"
        self.assertTrue(printable.isprintable())
        payload = self.valid_text(secret_key=printable).encode("utf-8")

        values = parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

        self.assertEqual(values["APCA_API_SECRET_KEY"], printable)

    def test_parser_wraps_the_explicit_approved_key_set(self) -> None:
        values = parse_exact_literal_assignments(
            b"SYNTHETIC=value\n",
            approved=frozenset({"SYNTHETIC"}),
        )

        self.assertEqual(values["SYNTHETIC"], "value")
        self.assertIn("SYNTHETIC", repr(values))

    def test_missing_path_and_descriptor_errors_are_secret_safe(self) -> None:
        canary = "CANARY_PATH_VALUE_MUST_NOT_ESCAPE"

        with self.assertRaises(LiteralEnvironmentError) as raised:
            load_literal_environment(self.root / canary)

        rendered = repr(raised.exception)
        self.assertNotIn(canary, rendered)
        self.assertEqual(str(raised.exception), "private environment is unavailable")

    def test_open_descriptor_survives_path_swap(self) -> None:
        original = self.private_file(self.valid_text(key_id="descriptor-original"))
        moved = self.root / "moved.env"
        replacement = self.valid_text(key_id="path-replacement")
        real_fstat = os.fstat
        swapped = False

        def swap_after_open(descriptor: int):
            nonlocal swapped
            if not swapped:
                original.rename(moved)
                original.write_text(replacement, encoding="utf-8")
                original.chmod(0o600)
                swapped = True
            return real_fstat(descriptor)

        with patch.object(unattended.os, "fstat", side_effect=swap_after_open):
            values = load_literal_environment(original)

        self.assertEqual(values["APCA_API_KEY_ID"], "descriptor-original")
        self.assertIn("path-replacement", original.read_text(encoding="utf-8"))

    def test_descriptor_metadata_must_remain_stable_during_read(self) -> None:
        path = self.private_file(self.valid_text())
        fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )
        real_fstat = os.fstat

        for changed_field in fields:
            calls = 0

            def changing_fstat(
                descriptor: int,
                *,
                field: str = changed_field,
            ) -> SimpleNamespace | os.stat_result:
                nonlocal calls
                calls += 1
                value = real_fstat(descriptor)
                if calls == 1:
                    return value
                metadata = {name: getattr(value, name) for name in fields}
                metadata[field] += 1
                return SimpleNamespace(**metadata)

            with self.subTest(field=changed_field), patch.object(
                unattended.os,
                "fstat",
                side_effect=changing_fstat,
            ):
                with self.assertRaises(LiteralEnvironmentError):
                    load_literal_environment(path)

    def test_descriptor_size_must_equal_the_bytes_read(self) -> None:
        path = self.private_file(self.valid_text())
        real_fstat = os.fstat
        fields = (
            "st_dev",
            "st_ino",
            "st_mode",
            "st_uid",
            "st_nlink",
            "st_size",
            "st_mtime_ns",
            "st_ctime_ns",
        )

        def inconsistent_size(descriptor: int) -> SimpleNamespace:
            value = real_fstat(descriptor)
            metadata = {name: getattr(value, name) for name in fields}
            metadata["st_size"] += 1
            return SimpleNamespace(**metadata)

        with patch.object(
            unattended.os,
            "fstat",
            side_effect=inconsistent_size,
        ):
            with self.assertRaises(LiteralEnvironmentError):
                load_literal_environment(path)

    def test_read_bounded_descriptor_rejects_one_byte_over_limit(self) -> None:
        descriptor = os.open(
            self.private_file(b"12345", name="bounded.bin"),
            os.O_RDONLY | os.O_CLOEXEC,
        )
        self.addCleanup(os.close, descriptor)

        with self.assertRaises(LiteralEnvironmentError):
            read_bounded_descriptor(descriptor, maximum_bytes=4)

    def test_parser_never_embeds_values_in_errors(self) -> None:
        canary = "CANARY_LITERAL_VALUE_MUST_NOT_ESCAPE"
        payload = self.valid_text(key_id=canary).encode("utf-8") + b"UNKNOWN=value\n"

        with self.assertRaises(LiteralEnvironmentError) as raised:
            parse_exact_literal_assignments(payload, approved=APPROVED_KEYS)

        self.assertNotIn(canary, repr(raised.exception))

    def test_loaded_mapping_redacts_every_implicit_representation(self) -> None:
        canaries = (
            "CANARY_REPR_KEY_MUST_NOT_ESCAPE",
            "CANARY_REPR_SECRET_MUST_NOT_ESCAPE",
            "CANARY_REPR_AGENT_MUST_NOT_ESCAPE",
            "CANARY_REPR_HOME_MUST_NOT_ESCAPE",
        )
        path = self.private_file(
            self.valid_text(
                key_id=canaries[0],
                secret_key=canaries[1],
                user_agent=f"Stock Monitor {canaries[2]} operator@example.com",
                home=str(self.root / canaries[3]),
            )
        )

        values = load_literal_environment(path)
        error = RuntimeError(values)
        rendered = (
            repr(values),
            str(values),
            f"{values}",
            f"{values!r}",
            format(values),
            str(error),
            repr(error),
            f"exception={error}",
        )

        for representation in rendered:
            for canary in canaries:
                with self.subTest(representation=representation, canary=canary):
                    self.assertNotIn(canary, representation)
        for key in APPROVED_KEYS:
            self.assertIn(key, repr(values))

    def test_loaded_mapping_is_read_only_without_a_mutable_backing(self) -> None:
        values = load_literal_environment(
            self.private_file(self.valid_text(secret_key="trusted-secret"))
        )

        self.assertIsInstance(values, Mapping)
        self.assertNotIsInstance(values, MutableMapping)
        self.assertFalse(hasattr(values, "__dict__"))
        self.assertFalse(hasattr(values, "clear"))
        self.assertFalse(hasattr(values, "update"))
        with self.assertRaises(TypeError):
            values["APCA_API_SECRET_KEY"] = "replacement"  # type: ignore[index]
        self.assertEqual(values["APCA_API_SECRET_KEY"], "trusted-secret")

    def test_mapping_storage_cannot_change_before_cli_run(self) -> None:
        values = load_literal_environment(
            self.private_file(self.valid_text(secret_key="trusted-secret"))
        )
        storage_names = tuple(
            name for name in dir(values) if name.endswith("entries")
        )
        replacement = tuple(
            (key, "attacker-controlled") for key in sorted(APPROVED_KEYS)
        )

        self.assertTrue(storage_names)
        for name in storage_names:
            with self.subTest(name=name, operation="assign"):
                with self.assertRaises(AttributeError):
                    setattr(values, name, replacement)
            with self.subTest(name=name, operation="delete"):
                with self.assertRaises(AttributeError):
                    delattr(values, name)

        observed: list[str] = []

        def run_with_environment(argv, *, environ):
            observed.append(environ["APCA_API_SECRET_KEY"])
            return 0

        with patch.object(unattended, "load_literal_environment", return_value=values):
            with patch.object(unattended.cli, "run", side_effect=run_with_environment):
                code = unattended.main(("provider", "smoke"))

        self.assertEqual(code, 0)
        self.assertEqual(observed, ["trusted-secret"])

    def test_loaded_mapping_remains_compatible_with_configuration_loader(self) -> None:
        path = self.private_file(
            self.valid_text(
                key_id="compatible-key",
                secret_key="compatible-secret",
                home=str(self.root / "configuration-home"),
            )
        )
        values = load_literal_environment(path)

        settings = load_settings(ROOT, values)

        self.assertEqual(settings.alpaca_api_key_id, "compatible-key")
        self.assertEqual(settings.alpaca_api_secret_key, "compatible-secret")

    def test_main_passes_only_loaded_values_to_cli(self) -> None:
        values = {
            "APCA_API_KEY_ID": "key",
            "APCA_API_SECRET_KEY": "secret",
            "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
            "STOCK_MONITOR_HOME": str(self.root / "state"),
        }
        with patch.object(unattended, "load_literal_environment", return_value=values):
            with patch.object(unattended.cli, "run", return_value=7) as run:
                result = unattended.main(("provider", "smoke", "--json"))

        self.assertEqual(result, 7)
        run.assert_called_once_with(
            ("provider", "smoke", "--json"),
            environ=values,
        )

    def test_main_rejects_an_interpreter_outside_the_module_root_venv(self) -> None:
        canary = "CANARY_WRONG_INTERPRETER_MUST_NOT_ESCAPE"
        values = {
            "APCA_API_KEY_ID": "key",
            "APCA_API_SECRET_KEY": "secret",
            "SEC_USER_AGENT": "Stock Monitor test operator@example.com",
            "STOCK_MONITOR_HOME": str(self.root / "state"),
        }
        stdout = StringIO()
        stderr = StringIO()
        with patch.object(
            sys,
            "executable",
            str(self.root / canary / "bin" / "python3"),
        ), patch.object(
            unattended,
            "load_literal_environment",
            return_value=values,
        ) as load, patch.object(
            unattended.cli,
            "run",
            return_value=7,
        ) as run, redirect_stdout(stdout), redirect_stderr(stderr):
            code = unattended.main(("provider", "smoke", "--json"))

        combined = stdout.getvalue() + stderr.getvalue()
        self.assertEqual(code, 2)
        self.assertEqual(
            stdout.getvalue(),
            "CONFIGURATION REQUIRED\nNo candidate or action was produced.\n",
        )
        self.assertEqual(stderr.getvalue(), "")
        self.assertNotIn(canary, combined)
        self.assertNotIn("Traceback", combined)
        load.assert_not_called()
        run.assert_not_called()

    def test_main_maps_loader_and_internal_errors_without_tracebacks(self) -> None:
        canary = "CANARY_UNATTENDED_EXCEPTION_MUST_NOT_ESCAPE"
        cases = (
            (
                LiteralEnvironmentError(canary),
                2,
                "CONFIGURATION REQUIRED\nNo candidate or action was produced.\n",
            ),
            (
                RuntimeError(canary),
                10,
                "INTERNAL ERROR\nNo candidate or action was produced.\n",
            ),
        )
        for error, expected_code, expected_output in cases:
            with self.subTest(error=type(error).__name__):
                stdout = StringIO()
                stderr = StringIO()
                with patch.object(
                    unattended,
                    "load_literal_environment",
                    side_effect=error,
                ), redirect_stdout(stdout), redirect_stderr(stderr):
                    code = unattended.main(("provider", "smoke"))

                combined = stdout.getvalue() + stderr.getvalue()
                self.assertEqual(code, expected_code)
                self.assertEqual(stdout.getvalue(), expected_output)
                self.assertEqual(stderr.getvalue(), "")
                self.assertNotIn(canary, combined)
                self.assertNotIn("Traceback", combined)


class UnattendedLauncherTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def install_launcher(self) -> Path:
        launcher = self.root / "scripts" / "run_monitor_unattended.sh"
        launcher.parent.mkdir(parents=True)
        launcher.write_bytes(LAUNCHER.read_bytes())
        launcher.chmod(0o755)
        return launcher

    def test_launcher_bytes_and_mode_are_exact(self) -> None:
        self.assertEqual(LAUNCHER.read_bytes(), EXPECTED_LAUNCHER)
        self.assertEqual(stat.S_IMODE(LAUNCHER.stat().st_mode), 0o755)

    def test_launcher_requires_an_absolute_invocation_path(self) -> None:
        launcher = self.install_launcher()
        completed = subprocess.run(
            [str(launcher.relative_to(self.root))],
            cwd=self.root,
            env={"PATH": os.environ.get("PATH", "")},
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 2)
        self.assertEqual(completed.stdout + completed.stderr, "")

    def test_launcher_uses_an_empty_environment_and_preserves_arguments(self) -> None:
        launcher = self.install_launcher()
        python = self.root / ".venv" / "bin" / "python3"
        python.parent.mkdir(parents=True)
        python.write_text(
            "#!/bin/sh\n"
            "/usr/bin/env\n"
            "for argument in \"$@\"; do\n"
            "  printf 'ARG=%s\\n' \"$argument\"\n"
            "done\n",
            encoding="utf-8",
        )
        python.chmod(0o755)
        inherited = {
            "PATH": "/tmp/CANARY_PATH_MUST_NOT_BE_USED",
            "PYTHONPATH": "/tmp/CANARY_PYTHONPATH_MUST_NOT_ESCAPE",
            "HTTPS_PROXY": "http://CANARY_PROXY_MUST_NOT_ESCAPE.invalid",
            "APCA_API_KEY_ID": "CANARY_KEY_MUST_NOT_ESCAPE",
            "APCA_API_SECRET_KEY": "CANARY_SECRET_MUST_NOT_ESCAPE",
            "AWS_SECRET_ACCESS_KEY": "CANARY_CLOUD_CREDENTIAL_MUST_NOT_ESCAPE",
            "ROBINHOOD_PASSWORD": "CANARY_BROKER_CREDENTIAL_MUST_NOT_ESCAPE",
            "HOME": "/tmp/CANARY_HOME_MUST_NOT_ESCAPE",
        }

        completed = subprocess.run(
            [str(launcher), "provider", "smoke", "--json"],
            cwd=self.root,
            env=inherited,
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)
        output = completed.stdout + completed.stderr
        for canary in inherited.values():
            with self.subTest(canary=canary):
                self.assertNotIn(canary, output)
        self.assertEqual(
            [line for line in completed.stdout.splitlines() if line.startswith("ARG=")],
            [
                "ARG=-I",
                "ARG=-m",
                "ARG=stock_monitor.unattended",
                "ARG=provider",
                "ARG=smoke",
                "ARG=--json",
            ],
        )

    def test_launcher_never_falls_back_to_path_python(self) -> None:
        launcher = self.install_launcher()
        marker = self.root / "path-python-ran"
        path_python = self.root / "bin" / "python3"
        path_python.parent.mkdir(parents=True)
        path_python.write_text(
            f"#!/bin/sh\n/usr/bin/touch '{marker}'\n",
            encoding="utf-8",
        )
        path_python.chmod(0o755)

        completed = subprocess.run(
            [str(launcher)],
            cwd=self.root,
            env={"PATH": str(path_python.parent)},
            text=True,
            capture_output=True,
            check=False,
        )

        self.assertEqual(completed.returncode, 2)
        self.assertFalse(marker.exists())

    def test_copied_launcher_runs_actual_cli_in_its_isolated_venv(self) -> None:
        launcher = self.install_launcher()
        venv_root = self.root / ".venv"
        setup = subprocess.run(
            [
                sys.executable,
                "-m",
                "venv",
                "--without-pip",
                str(venv_root),
            ],
            cwd=self.root,
            env={"PATH": os.environ.get("PATH", "")},
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
        self.assertEqual(setup.returncode, 0, setup.stdout + setup.stderr)

        python = venv_root / "bin" / "python3"
        purelib_result = subprocess.run(
            [
                str(python),
                "-I",
                "-c",
                "import sysconfig; print(sysconfig.get_path('purelib'))",
            ],
            cwd=self.root,
            env={},
            text=True,
            capture_output=True,
            timeout=10,
            check=False,
        )
        self.assertEqual(
            purelib_result.returncode,
            0,
            purelib_result.stdout + purelib_result.stderr,
        )
        purelib = Path(purelib_result.stdout.strip())

        source_root = self.root / "src"
        shutil.copytree(
            ROOT / "src" / "stock_monitor",
            source_root / "stock_monitor",
            ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
        )
        shutil.copytree(ROOT / "config", self.root / "config")
        (purelib / "stock-monitor-test.pth").write_text(
            f"{source_root}\n",
            encoding="utf-8",
        )

        runtime = self.root / "runtime"
        file_canaries = (
            "CANARY_E2E_FILE_KEY_MUST_NOT_ESCAPE",
            "CANARY_E2E_FILE_SECRET_MUST_NOT_ESCAPE",
        )
        private_environment = self.root / ".env"
        private_environment.write_text(
            "APCA_API_KEY_ID=" + file_canaries[0] + "\n"
            "APCA_API_SECRET_KEY=" + file_canaries[1] + "\n"
            "SEC_USER_AGENT=Stock Monitor e2e operator@example.com\n"
            f"STOCK_MONITOR_HOME={runtime}\n",
            encoding="utf-8",
        )
        private_environment.chmod(0o600)

        inherited_runtime = self.root / "inherited-runtime"
        inherited = {
            "PATH": "/tmp/CANARY_E2E_PATH_MUST_NOT_BE_USED",
            "PYTHONPATH": "/tmp/CANARY_E2E_PYTHONPATH_MUST_NOT_ESCAPE",
            "HTTPS_PROXY": "http://CANARY_E2E_PROXY_MUST_NOT_ESCAPE.invalid",
            "APCA_API_KEY_ID": "CANARY_E2E_INHERITED_KEY_MUST_NOT_ESCAPE",
            "APCA_API_SECRET_KEY": "CANARY_E2E_INHERITED_SECRET_MUST_NOT_ESCAPE",
            "STOCK_MONITOR_HOME": str(inherited_runtime),
        }
        outside = self.root / "outside"
        outside.mkdir()

        completed = subprocess.run(
            [str(launcher), "db", "init", "--json"],
            cwd=outside,
            env=inherited,
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )

        self.assertEqual(completed.returncode, 0, completed.stdout + completed.stderr)
        self.assertEqual(completed.stderr, "")
        result = json.loads(completed.stdout)
        self.assertEqual(set(result), {"migration_count", "status"})
        self.assertEqual(result["status"], "INITIALIZED")
        self.assertIsInstance(result["migration_count"], int)
        self.assertGreater(result["migration_count"], 0)
        combined = completed.stdout + completed.stderr
        for canary in (*file_canaries, *inherited.values()):
            with self.subTest(canary=canary):
                self.assertNotIn(canary, combined)
        self.assertTrue((runtime / ".stock-monitor" / "journal.sqlite3").is_file())
        self.assertFalse(inherited_runtime.exists())
        self.assertFalse((self.root / ".stock-monitor").exists())


if __name__ == "__main__":
    unittest.main()
