from __future__ import annotations

import ast
import re
import tomllib
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[2]
_FORBIDDEN_MODULE_TOKENS = {
    "broker",
    "brokerage",
    "execution",
    "executions",
    "order",
    "orders",
    "trading",
}
_ORDER_ACTION_TOKENS = {
    "buy",
    "cancel",
    "create",
    "execute",
    "place",
    "replace",
    "route",
    "send",
    "sell",
    "submit",
}
_CAPABILITY_TOKENS = {
    "adapter",
    "api",
    "client",
    "engine",
    "executor",
    "gateway",
    "provider",
    "service",
}
_PROHIBITED_ENDPOINTS = (
    re.compile(r"/(?:v[0-9]+/)?orders(?:[/?#]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[./])robinhood\.(?:com|net)\.?(?:[:/]|$)", re.IGNORECASE),
)
_ALPACA_HOST = re.compile(
    r"(?<![a-z0-9-])(?P<host>(?:[a-z0-9-]+\.)+alpaca\.markets)"
    r"\.?(?::[0-9]+)?(?=$|[/?#\s'\"])",
    re.IGNORECASE,
)
_APPROVED_ALPACA_DATA_HOST = "data.alpaca.markets"
_HTTP_PROVIDER_PATH = Path("providers/http.py")
_THIRD_PARTY_HTTP_MODULES = frozenset(
    {"aiohttp", "httpcore", "httpx", "requests", "urllib3"}
)
_DIRECT_NETWORK_MODULES = frozenset(
    {
        "ftplib",
        "http.client",
        "http.server",
        "imaplib",
        "nntplib",
        "poplib",
        "smtplib",
        "socket",
        "socketserver",
        "telnetlib",
        "xmlrpc.client",
    }
)
_ASYNCIO_NETWORK_CALLS = frozenset(
    {
        "asyncio.open_connection",
        "asyncio.open_unix_connection",
        "asyncio.start_server",
        "asyncio.start_unix_server",
    }
)
_APPROVED_URLLIB_CALLS = frozenset(
    {"urllib.request.Request", "urllib.request.urlopen"}
)
_APPROVED_URLLIB_CONFIGURATION = frozenset(
    {"urllib.request.HTTPRedirectHandler", "urllib.request.build_opener"}
)
_APPROVED_URLLIB_IMPORTS = frozenset(
    {
        "urllib.request",
        *_APPROVED_URLLIB_CALLS,
        *_APPROVED_URLLIB_CONFIGURATION,
    }
)


def _identifier_tokens(name: str) -> set[str]:
    pieces = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name).replace("-", "_")
    return {piece.lower() for piece in pieces.split("_") if piece}


def _is_forbidden_api(name: str) -> bool:
    tokens = _identifier_tokens(name)
    if tokens.intersection({"order", "orders"}) and tokens.intersection(
        _ORDER_ACTION_TOKENS | _CAPABILITY_TOKENS
    ):
        return True
    if "execute" in tokens and (
        len(tokens) == 1
        or tokens.intersection({"buy", "order", "orders", "sell", "trade"})
    ):
        return True
    if tokens.intersection({"trade", "trades"}) and tokens.intersection(
        _ORDER_ACTION_TOKENS | {"executor"}
    ):
        return True
    if tokens.intersection(
        {"broker", "brokerage", "execution", "trading"}
    ) and tokens.intersection(_CAPABILITY_TOKENS):
        return True
    return False


def _module_name_is_forbidden(name: str) -> bool:
    return any(
        _identifier_tokens(segment).intersection(_FORBIDDEN_MODULE_TOKENS)
        for segment in name.split(".")
    )


def _has_forbidden_import(node: ast.Import | ast.ImportFrom) -> bool:
    if isinstance(node, ast.Import):
        return any(
            _module_name_is_forbidden(alias.name)
            or (
                alias.asname is not None
                and (
                    _module_name_is_forbidden(alias.asname)
                    or _is_forbidden_api(alias.asname)
                )
            )
            for alias in node.names
        )
    if node.module and _module_name_is_forbidden(node.module):
        return True
    return any(
        alias.name.lower() in _FORBIDDEN_MODULE_TOKENS
        or _is_forbidden_api(alias.name)
        or (
            alias.asname is not None
            and (
                _module_name_is_forbidden(alias.asname)
                or _is_forbidden_api(alias.asname)
            )
        )
        for alias in node.names
    )


def _docstring_constants(tree: ast.AST) -> set[int]:
    docstrings: set[int] = set()
    owners = (ast.Module, ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)
    for node in ast.walk(tree):
        if not isinstance(node, owners) or not node.body:
            continue
        first = node.body[0]
        if (
            isinstance(first, ast.Expr)
            and isinstance(first.value, ast.Constant)
            and isinstance(first.value.value, str)
        ):
            docstrings.add(id(first.value))
    return docstrings


def _qualified_name_tokens(node: ast.AST) -> set[str]:
    if isinstance(node, ast.Name):
        return _identifier_tokens(node.id)
    if isinstance(node, ast.Attribute):
        return _qualified_name_tokens(node.value) | _identifier_tokens(node.attr)
    return set()


def _brokerage_action(call: ast.Call) -> str | None:
    if not isinstance(call.func, ast.Attribute):
        return None
    receiver_tokens = _qualified_name_tokens(call.func.value)
    action_tokens = _identifier_tokens(call.func.attr)
    if (
        receiver_tokens.intersection(_FORBIDDEN_MODULE_TOKENS)
        and action_tokens.intersection(_ORDER_ACTION_TOKENS)
    ):
        return call.func.attr
    return None


def _imported_paths(node: ast.Import | ast.ImportFrom) -> tuple[str, ...]:
    if isinstance(node, ast.Import):
        return tuple(alias.name for alias in node.names)
    if node.module is None:
        return ()
    return tuple(f"{node.module}.{alias.name}" for alias in node.names)


def _import_aliases(tree: ast.AST) -> dict[str, str]:
    aliases: dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for imported in node.names:
                if imported.asname is not None:
                    aliases[imported.asname] = imported.name
                else:
                    root = imported.name.split(".", 1)[0]
                    aliases[root] = root
        elif isinstance(node, ast.ImportFrom) and node.module:
            for imported in node.names:
                if imported.name == "*":
                    continue
                aliases[imported.asname or imported.name] = (
                    f"{node.module}.{imported.name}"
                )
    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            if value is None or isinstance(value, ast.Call):
                continue
            resolved = _qualified_name(value, aliases)
            if resolved is None or not _could_lead_to_network(resolved):
                continue
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                if (
                    isinstance(target, ast.Name)
                    and aliases.get(target.id) != resolved
                ):
                    aliases[target.id] = resolved
                    changed = True
    return aliases


def _qualified_name(
    node: ast.AST,
    aliases: dict[str, str],
    provider_exports: dict[str, str] | None = None,
) -> str | None:
    if isinstance(node, ast.Name):
        result = aliases.get(node.id, node.id)
    elif isinstance(node, ast.Attribute):
        owner = _qualified_name(node.value, aliases, provider_exports)
        if owner is not None:
            result = f"{owner}.{node.attr}"
        else:
            return None
    else:
        return None
    return _canonical_network_export(result, provider_exports or {})


def _module_matches(path: str, module: str) -> bool:
    return path == module or path.startswith(f"{module}.")


def _is_network_path(path: str) -> bool:
    root = path.split(".", 1)[0]
    if root in _THIRD_PARTY_HTTP_MODULES:
        return True
    if any(_module_matches(path, module) for module in _DIRECT_NETWORK_MODULES):
        return True
    if _module_matches(path, "urllib.request"):
        return True
    return path in _ASYNCIO_NETWORK_CALLS


def _could_lead_to_network(path: str) -> bool:
    if _is_network_path(path):
        return True
    candidates = {
        "urllib.request",
        *_DIRECT_NETWORK_MODULES,
        *_THIRD_PARTY_HTTP_MODULES,
    }
    return any(candidate.startswith(f"{path}.") for candidate in candidates)


def _canonical_network_export(
    path: str,
    provider_exports: dict[str, str],
) -> str:
    for local_name, network_path in provider_exports.items():
        prefixes = (
            f"providers.http.{local_name}",
            f"stock_monitor.providers.http.{local_name}",
        )
        for prefix in prefixes:
            if path == prefix or path.startswith(f"{prefix}."):
                return network_path + path[len(prefix) :]
    return path


def _provider_network_exports(package: Path) -> dict[str, str]:
    provider = package / _HTTP_PROVIDER_PATH
    if not provider.is_file():
        return {}
    tree = ast.parse(provider.read_text(encoding="utf-8"), filename=str(provider))
    aliases = _import_aliases(tree)
    return {
        local_name: path
        for local_name, path in aliases.items()
        if _could_lead_to_network(path)
    }


def _network_import_is_allowed(path: str, relative: Path) -> bool:
    return relative == _HTTP_PROVIDER_PATH and path in _APPROVED_URLLIB_IMPORTS


def _network_import_violations(
    node: ast.Import | ast.ImportFrom,
    relative: Path,
    provider_exports: dict[str, str],
) -> list[str]:
    violations: list[str] = []
    if (
        isinstance(node, ast.ImportFrom)
        and node.module is not None
        and node.module.endswith("providers.http")
        and any(imported.name == "*" for imported in node.names)
        and provider_exports
    ):
        violations.append("raw provider star import")
    for imported_path in _imported_paths(node):
        path = _canonical_network_export(imported_path, provider_exports)
        if _is_network_path(path) and not _network_import_is_allowed(path, relative):
            violations.append(path)
    return violations


def _is_none_literal(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


def _is_get_or_none_literal(node: ast.AST) -> bool:
    return _is_none_literal(node) or (
        isinstance(node, ast.Constant)
        and isinstance(node.value, str)
        and node.value == "GET"
    )


def _has_dynamic_arguments(call: ast.Call) -> bool:
    return any(isinstance(argument, ast.Starred) for argument in call.args) or any(
        keyword.arg is None for keyword in call.keywords
    )


def _keyword_values(call: ast.Call, names: set[str]) -> tuple[ast.AST, ...]:
    return tuple(
        keyword.value for keyword in call.keywords if keyword.arg in names
    )


def _request_is_get_only(call: ast.Call) -> bool:
    if _has_dynamic_arguments(call) or len(call.args) > 6:
        return False
    if len(call.args) >= 2 and not _is_none_literal(call.args[1]):
        return False
    if any(
        not _is_none_literal(value)
        for value in _keyword_values(call, {"body", "data"})
    ):
        return False
    if len(call.args) >= 6 and not _is_get_or_none_literal(call.args[5]):
        return False
    return all(
        _is_get_or_none_literal(value)
        for value in _keyword_values(call, {"method"})
    )


def _urlopen_is_get_only(
    call: ast.Call,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
    request_names: set[str],
) -> bool:
    if _has_dynamic_arguments(call) or not call.args or len(call.args) > 7:
        return False
    if len(call.args) >= 2 and not _is_none_literal(call.args[1]):
        return False
    if not all(
        _is_none_literal(value)
        for value in _keyword_values(call, {"body", "data"})
    ):
        return False

    request = call.args[0]
    if _static_string(request) is not None:
        return True
    if isinstance(request, ast.Call):
        return (
            _qualified_name(request.func, aliases, provider_exports)
            == "urllib.request.Request"
            and _request_is_get_only(request)
        )
    return _qualified_name(request, aliases, provider_exports) in request_names


def _attribute_open_has_body(call: ast.Call) -> bool:
    return len(call.args) >= 2 or any(
        keyword.arg in {"body", "data"} for keyword in call.keywords
    )


def _attribute_open_has_tracked_request(
    call: ast.Call,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
    request_names: set[str],
) -> bool:
    if not call.args:
        return False
    request = call.args[0]
    if isinstance(request, ast.Call):
        return (
            _qualified_name(request.func, aliases, provider_exports)
            == "urllib.request.Request"
        )
    return _qualified_name(request, aliases, provider_exports) in request_names


def _assigned_targets(node: ast.Assign | ast.AnnAssign) -> tuple[ast.AST, ...]:
    if isinstance(node, ast.Assign):
        return tuple(node.targets)
    return (node.target,)


def _constructed_names(
    tree: ast.AST,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
    constructor: str,
) -> set[str]:
    names: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Assign, ast.AnnAssign)):
            continue
        value = node.value
        if not isinstance(value, ast.Call):
            continue
        if _qualified_name(value.func, aliases, provider_exports) != constructor:
            continue
        for target in _assigned_targets(node):
            name = _qualified_name(target, aliases, provider_exports)
            if name is not None:
                names.add(name)
    return names


def _expanded_object_names(
    tree: ast.AST,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
    initial: set[str],
) -> set[str]:
    names = set(initial)
    changed = True
    while changed:
        changed = False
        for node in ast.walk(tree):
            if not isinstance(node, (ast.Assign, ast.AnnAssign)):
                continue
            value = node.value
            if value is None or isinstance(value, ast.Call):
                continue
            source = _qualified_name(value, aliases, provider_exports)
            if source not in names:
                continue
            for target in _assigned_targets(node):
                target_name = _qualified_name(target, aliases, provider_exports)
                if target_name is not None and target_name not in names:
                    names.add(target_name)
                    changed = True
    return names


def _request_subclass_violation(
    node: ast.ClassDef,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
) -> bool:
    return any(
        _qualified_name(base, aliases, provider_exports)
        == "urllib.request.Request"
        for base in node.bases
    )


def _request_mutation_violation(
    node: ast.AST,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
    request_names: set[str],
) -> bool:
    if isinstance(node, ast.Call):
        setter = _qualified_name(node.func, aliases, provider_exports)
        if setter not in {"setattr", "builtins.setattr"} or len(node.args) < 3:
            return False
        owner = _qualified_name(node.args[0], aliases, provider_exports)
        attribute = _static_string(node.args[1])
        if owner not in request_names or attribute is None:
            return False
        if attribute in {"body", "data"}:
            return not _is_none_literal(node.args[2])
        if attribute == "method":
            return not _is_get_or_none_literal(node.args[2])
        return attribute == "get_method"

    if isinstance(node, ast.AugAssign):
        targets = (node.target,)
        value = node.value
    elif isinstance(node, (ast.Assign, ast.AnnAssign)):
        targets = _assigned_targets(node)
        value = node.value
        if value is None:
            return False
    else:
        return False

    for target in targets:
        if not isinstance(target, ast.Attribute):
            continue
        owner = _qualified_name(target.value, aliases, provider_exports)
        if owner not in request_names:
            continue
        if target.attr in {"body", "data"} and not _is_none_literal(value):
            return True
        if target.attr == "method" and not _is_get_or_none_literal(value):
            return True
        if target.attr == "get_method":
            return True
    return False


def _dynamic_network_import(
    call: ast.Call,
    aliases: dict[str, str],
    provider_exports: dict[str, str],
) -> str | None:
    importer = _qualified_name(call.func, aliases, provider_exports)
    if importer not in {"__import__", "builtins.__import__", "importlib.import_module"}:
        return None
    if not call.args:
        return "dynamic import"
    imported_path = _static_string(call.args[0])
    if imported_path is None:
        return "dynamic import"
    if _could_lead_to_network(imported_path):
        return imported_path
    return None


def _network_call_violation(
    call: ast.Call,
    aliases: dict[str, str],
    relative: Path,
    provider_exports: dict[str, str],
    opener_names: set[str],
    request_names: set[str],
) -> str | None:
    path = _qualified_name(call.func, aliases, provider_exports)
    if isinstance(call.func, ast.Attribute) and call.func.attr == "open":
        owner = _qualified_name(call.func.value, aliases, provider_exports)
        directly_constructed = (
            isinstance(call.func.value, ast.Call)
            and _qualified_name(
                call.func.value.func,
                aliases,
                provider_exports,
            )
            == "urllib.request.build_opener"
        )
        approved_opener = owner in opener_names or directly_constructed
        has_body = _attribute_open_has_body(call)
        has_tracked_request = _attribute_open_has_tracked_request(
            call,
            aliases,
            provider_exports,
            request_names,
        )
        if relative == _HTTP_PROVIDER_PATH and (has_body or has_tracked_request):
            if (
                not approved_opener
                or has_body
                or not _urlopen_is_get_only(
                    call,
                    aliases,
                    provider_exports,
                    request_names,
                )
            ):
                return "opener.open"
            return None
        if approved_opener:
            if relative != _HTTP_PROVIDER_PATH or not _urlopen_is_get_only(
                call,
                aliases,
                provider_exports,
                request_names,
            ):
                return "opener.open"
            return None

    if path is None:
        return None

    bare_primitive = path in {"Request", "urlopen"}
    if not bare_primitive and not _is_network_path(path):
        return None
    if relative != _HTTP_PROVIDER_PATH:
        return path
    if path == "urllib.request.Request":
        return None if _request_is_get_only(call) else path
    if path == "urllib.request.urlopen":
        return (
            None
            if _urlopen_is_get_only(
                call,
                aliases,
                provider_exports,
                request_names,
            )
            else path
        )
    if path in _APPROVED_URLLIB_CONFIGURATION:
        return None
    return path


def _has_prohibited_endpoint(value: str) -> bool:
    if any(pattern.search(value) for pattern in _PROHIBITED_ENDPOINTS):
        return True
    return any(
        match.group("host").lower() != _APPROVED_ALPACA_DATA_HOST
        for match in _ALPACA_HOST.finditer(value)
    )


def _static_string(node: ast.AST) -> str | None:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.Add):
        left = _static_string(node.left)
        right = _static_string(node.right)
        if left is not None and right is not None:
            return left + right
    if isinstance(node, ast.JoinedStr):
        parts = [_static_string(value) for value in node.values]
        if all(part is not None for part in parts):
            return "".join(part for part in parts if part is not None)
    return None


def _boundary_violations(package: Path) -> list[str]:
    violations: list[str] = []
    provider_exports = _provider_network_exports(package)
    for path in sorted(package.rglob("*.py")):
        relative = path.relative_to(package)
        module_tokens: set[str] = set()
        for segment in relative.with_suffix("").parts:
            module_tokens.update(_identifier_tokens(segment))
        if module_tokens.intersection(_FORBIDDEN_MODULE_TOKENS):
            violations.append(f"forbidden module:{relative}:0:module path")

        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        docstrings = _docstring_constants(tree)
        aliases = _import_aliases(tree)
        opener_names = _constructed_names(
            tree,
            aliases,
            provider_exports,
            "urllib.request.build_opener",
        )
        request_names = _constructed_names(
            tree,
            aliases,
            provider_exports,
            "urllib.request.Request",
        )
        request_names = _expanded_object_names(
            tree,
            aliases,
            provider_exports,
            request_names,
        )
        for node in ast.walk(tree):
            line = getattr(node, "lineno", 0)
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                if _has_forbidden_import(node):
                    violations.append(f"forbidden import:{relative}:{line}:import")
                for network_path in _network_import_violations(
                    node,
                    relative,
                    provider_exports,
                ):
                    violations.append(
                        f"network capability:{relative}:{line}:{network_path}"
                    )
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if _is_forbidden_api(node.name):
                    violations.append(
                        f"forbidden API:{relative}:{line}:{node.name}"
                    )
            if isinstance(node, ast.ClassDef) and _request_subclass_violation(
                node,
                aliases,
                provider_exports,
            ):
                violations.append(
                    f"network capability:{relative}:{line}:Request subclass"
                )
            if isinstance(node, ast.Attribute) and _is_forbidden_api(node.attr):
                violations.append(f"forbidden API:{relative}:{line}:{node.attr}")
            if isinstance(node, ast.Call):
                action = _brokerage_action(node)
                if action is not None:
                    violations.append(f"forbidden API:{relative}:{line}:{action}")
                network_path = _dynamic_network_import(
                    node,
                    aliases,
                    provider_exports,
                ) or _network_call_violation(
                    node,
                    aliases,
                    relative,
                    provider_exports,
                    opener_names,
                    request_names,
                )
                if network_path is not None:
                    violations.append(
                        f"network capability:{relative}:{line}:{network_path}"
                    )
            if _request_mutation_violation(
                node,
                aliases,
                provider_exports,
                request_names,
            ):
                violations.append(
                    f"network capability:{relative}:{line}:Request mutation"
                )
            static_text = _static_string(node)
            if (
                static_text is not None
                and id(node) not in docstrings
                and _has_prohibited_endpoint(static_text)
            ):
                violations.append(
                    f"prohibited endpoint:{relative}:{line}:network target"
                )
    return violations


class BrokerageBoundaryTests(unittest.TestCase):
    def test_runtime_has_no_third_party_dependencies(self) -> None:
        data = tomllib.loads(
            (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )

        self.assertEqual(data["project"]["dependencies"], [])

    def test_project_uses_python_311_src_layout_and_safe_entry_point(self) -> None:
        data = tomllib.loads(
            (PROJECT_ROOT / "pyproject.toml").read_text(encoding="utf-8")
        )

        self.assertEqual(data["project"]["requires-python"], ">=3.11")
        self.assertEqual(
            data["project"]["scripts"]["stock-monitor"],
            "stock_monitor.cli:main",
        )
        self.assertEqual(data["tool"]["setuptools"]["package-dir"], {"": "src"})

    def test_source_configuration_excludes_trading_and_robinhood_hosts(self) -> None:
        text = (PROJECT_ROOT / "config" / "sources.toml").read_text(
            encoding="utf-8"
        ).lower()

        self.assertIn("data.alpaca.markets", text)
        self.assertNotIn("paper-api.alpaca.markets", text)
        self.assertNotIn("api.alpaca.markets", text)
        self.assertNotIn("robinhood", text)

    def test_package_defines_no_broker_or_order_module(self) -> None:
        package = PROJECT_ROOT / "src" / "stock_monitor"

        self.assertEqual(_boundary_violations(package), [])

    def test_boundary_scanner_recurses_and_detects_unsafe_capabilities(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            unsafe = package / "providers" / "deep" / "order_gateway.py"
            unsafe.parent.mkdir(parents=True)
            unsafe.write_text(
                "import vendor.execution\n"
                "class BrokerageClient:\n"
                "    def place_order(self, transport):\n"
                "        return transport.post(\n"
                "            'https://api.robinhood.com/v2/orders'\n"
                "        )\n"
                "class OrderClient:\n"
                "    pass\n"
                "def execute_trade():\n"
                "    pass\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        categories = {violation.split(":", 1)[0] for violation in violations}
        self.assertEqual(
            categories,
            {
                "forbidden API",
                "forbidden import",
                "forbidden module",
                "prohibited endpoint",
            },
        )
        self.assertTrue(
            any(violation.endswith(":execute_trade") for violation in violations)
        )
        self.assertTrue(
            any(violation.endswith(":OrderClient") for violation in violations)
        )

    def test_boundary_scanner_ignores_safe_documentation_strings(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            package.mkdir()
            (package / "safe.py").write_text(
                '"""POST and /v2/orders are prohibited examples, not capabilities."""\n'
                "from stock_monitor.domain import ExecutionEvent\n"
                "\n"
                "def get_only():\n"
                "    return 'GET'\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertEqual(violations, [])

    def test_boundary_scanner_rejects_indirect_order_apis_and_alpaca_trading_hosts(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            package.mkdir()
            (package / "provider.py").write_text(
                "TRADING_ORIGIN = 'https://broker-api.sandbox.alpaca.markets./'\n"
                "def build(vendor):\n"
                "    return vendor.OrderClient()\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertTrue(
            any(violation.endswith(":OrderClient") for violation in violations)
        )
        self.assertTrue(
            any(violation.startswith("prohibited endpoint:") for violation in violations)
        )

    def test_boundary_scanner_rejects_indirect_api_and_endpoint_shapes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            package.mkdir()
            (package / "provider.py").write_text(
                "class TradeExecutor:\n"
                "    pass\n"
                "class ExecutionProvider:\n"
                "    pass\n"
                "def submit_trade():\n"
                "    pass\n"
                "TARGET = (\n"
                "    'https://api.' + 'alpaca.' + 'markets/v2/' + 'orders'\n"
                ")\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        for name in ("TradeExecutor", "ExecutionProvider", "submit_trade"):
            with self.subTest(name=name):
                self.assertTrue(
                    any(violation.endswith(f":{name}") for violation in violations)
                )
        self.assertTrue(
            any(violation.startswith("prohibited endpoint:") for violation in violations)
        )

    def test_boundary_scanner_combines_brokerage_receivers_with_actions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            package.mkdir()
            (package / "provider.py").write_text(
                "import vendor as brokerage\n"
                "def run(broker):\n"
                "    broker.buy('AAPL', 1)\n"
                "    broker.sell('AAPL', 1)\n"
                "    broker.submit({'symbol': 'AAPL'})\n"
                "    brokerage.buy('AAPL', 1)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertTrue(
            any(violation.startswith("forbidden import:") for violation in violations)
        )
        for action in ("buy", "sell", "submit"):
            with self.subTest(action=action):
                self.assertTrue(
                    any(violation.endswith(f":{action}") for violation in violations)
                )

    def test_non_network_post_names_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            package.mkdir()
            (package / "journal.py").write_text(
                "MARKET_PHASE = 'POST'\n"
                "class Journal:\n"
                "    def post(self, event):\n"
                "        return event\n"
                "def record(journal, event):\n"
                "    return journal.post(event)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertEqual(violations, [])

    def test_non_network_open_names_are_allowed(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            provider = package / "providers" / "http.py"
            provider.parent.mkdir(parents=True)
            provider.write_text(
                "class Archive:\n"
                "    def open(self, record):\n"
                "        return record\n"
                "def read(archive, record):\n"
                "    return archive.open(record)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertEqual(violations, [])

    def test_network_imports_and_calls_are_rejected_outside_http_provider(self) -> None:
        fixtures = {
            "urllib import alias": (
                "import urllib.request as transport\n"
                "def fetch(url):\n"
                "    return transport.urlopen(transport.Request(url))\n"
            ),
            "from import aliases": (
                "from urllib.request import Request as R, urlopen as open_url\n"
                "def fetch(url):\n"
                "    return open_url(R(url))\n"
            ),
            "injected primitive calls": (
                "def fetch(Request, urlopen, url):\n"
                "    return urlopen(Request(url))\n"
            ),
        }
        for case, source in fixtures.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                package = Path(directory) / "stock_monitor"
                package.mkdir()
                (package / "feature.py").write_text(source, encoding="utf-8")

                violations = _boundary_violations(package)

                self.assertTrue(
                    any(
                        violation.startswith("network capability:")
                        for violation in violations
                    )
                )

    def test_designated_http_provider_allows_structural_get_only(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            provider = package / "providers" / "http.py"
            provider.parent.mkdir(parents=True)
            provider.write_text(
                "from urllib.request import Request as URLRequest\n"
                "from urllib.request import urlopen as open_url\n"
                "MARKET_DATA = 'https://data.alpaca.markets/v2/stocks'\n"
                "def fetch_default(url):\n"
                "    return open_url(URLRequest(url))\n"
                "def fetch_explicit(url):\n"
                "    request = URLRequest(url, data=None, method='GET')\n"
                "    return open_url(request, data=None)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertEqual(violations, [])

    def test_designated_http_provider_allows_planned_redirect_safe_opener(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            provider = package / "providers" / "http.py"
            provider.parent.mkdir(parents=True)
            provider.write_text(
                "import urllib.request as url_request\n"
                "class NoAutomaticRedirects(url_request.HTTPRedirectHandler):\n"
                "    def redirect_request(self, req, fp, code, msg, headers, newurl):\n"
                "        return None\n"
                "OPENER = url_request.build_opener(NoAutomaticRedirects)\n"
                "def fetch(url):\n"
                "    request = url_request.Request(url, method='GET')\n"
                "    return OPENER.open(request)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertEqual(violations, [])

    def test_designated_http_provider_rejects_factory_opener_body(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            provider = package / "providers" / "http.py"
            provider.parent.mkdir(parents=True)
            provider.write_text(
                "from urllib.request import Request, build_opener\n"
                "def make_opener():\n"
                "    return build_opener()\n"
                "OPENER = make_opener()\n"
                "def fetch(url, payload):\n"
                "    request = Request(url)\n"
                "    return OPENER.open(request, data=payload)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertTrue(
            any(violation.endswith(":opener.open") for violation in violations)
        )

    def test_designated_http_provider_rejects_dynamic_method_and_body_bypass(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            package = Path(directory) / "stock_monitor"
            provider = package / "providers" / "http.py"
            provider.parent.mkdir(parents=True)
            provider.write_text(
                "from urllib.request import Request, urlopen\n"
                "def fetch(url, payload, verb):\n"
                "    request = Request(url, data=payload, method=verb)\n"
                "    return urlopen(request)\n",
                encoding="utf-8",
            )

            violations = _boundary_violations(package)

        self.assertTrue(
            any(violation.startswith("network capability:") for violation in violations)
        )

    def test_designated_http_provider_rejects_non_get_and_dynamic_request_forms(self) -> None:
        calls = {
            "post": "Request(url, method='POST')",
            "put": "Request(url, method='PUT')",
            "patch": "Request(url, method='PATCH')",
            "delete": "Request(url, method='DELETE')",
            "head": "Request(url, method='HEAD')",
            "custom": "Request(url, method='CUSTOM')",
            "dynamic method": "Request(url, method=method)",
            "enum method": "Request(url, method=HTTPMethod.GET)",
            "positional body": "Request(url, payload)",
            "empty body": "Request(url, b'')",
            "keyword body": "Request(url, body=payload)",
            "urlopen data": "urlopen(url, data=payload)",
            "urlopen positional data": "urlopen(url, payload)",
            "positional method": (
                "Request(url, None, {}, None, False, 'POST')"
            ),
            "expanded options": "Request(url, **options)",
        }
        for case, call in calls.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                package = Path(directory) / "stock_monitor"
                provider = package / "providers" / "http.py"
                provider.parent.mkdir(parents=True)
                provider.write_text(
                    "from urllib.request import Request, urlopen\n"
                    "def fetch(url, payload=None, method=None, options=None):\n"
                    f"    return {call}\n",
                    encoding="utf-8",
                )

                violations = _boundary_violations(package)

                self.assertTrue(
                    any(
                        violation.startswith("network capability:")
                        for violation in violations
                    )
                )

    def test_designated_http_provider_rejects_alternate_network_clients(self) -> None:
        imports = {
            "http client": "import http.client as transport\n",
            "socket": "import socket as transport\n",
            "urllib urlretrieve": (
                "from urllib.request import urlretrieve as transport\n"
            ),
            "requests": "import requests as transport\n",
            "httpx": "import httpx as transport\n",
            "aiohttp": "from aiohttp import ClientSession as transport\n",
            "urllib3": "import urllib3 as transport\n",
            "xmlrpc": "import xmlrpc.client as transport\n",
            "asyncio socket": "from asyncio import open_connection as transport\n",
        }
        for case, source in imports.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                package = Path(directory) / "stock_monitor"
                provider = package / "providers" / "http.py"
                provider.parent.mkdir(parents=True)
                provider.write_text(source, encoding="utf-8")

                violations = _boundary_violations(package)

                self.assertTrue(
                    any(
                        violation.startswith("network capability:")
                        for violation in violations
                    )
                )

    def test_designated_http_provider_rejects_rebinding_mutation_and_subclasses(self) -> None:
        fixtures = {
            "rebound aliases": (
                "from urllib import request as url_request\n"
                "R = url_request.Request\n"
                "open_url = url_request.urlopen\n"
                "def fetch(url, payload, verb):\n"
                "    return open_url(R(url, data=payload, method=verb))\n"
            ),
            "request mutation": (
                "from urllib.request import Request, urlopen\n"
                "def fetch(url, payload, verb):\n"
                "    request = Request(url)\n"
                "    request.data = payload\n"
                "    request.method = verb\n"
                "    return urlopen(request)\n"
            ),
            "aliased request mutation": (
                "from urllib.request import Request, urlopen\n"
                "def fetch(url, payload):\n"
                "    request = Request(url)\n"
                "    alias = request\n"
                "    alias.data = payload\n"
                "    return urlopen(request)\n"
            ),
            "setattr request mutation": (
                "from urllib.request import Request, urlopen\n"
                "def fetch(url, payload, verb):\n"
                "    request = Request(url)\n"
                "    setattr(request, 'data', payload)\n"
                "    setattr(request, 'method', verb)\n"
                "    return urlopen(request)\n"
            ),
            "get method mutation": (
                "from urllib.request import Request, urlopen\n"
                "def fetch(url):\n"
                "    request = Request(url)\n"
                "    request.get_method = lambda: 'POST'\n"
                "    return urlopen(request)\n"
            ),
            "request subclass": (
                "from urllib.request import Request, urlopen\n"
                "class UnsafeRequest(Request):\n"
                "    def get_method(self):\n"
                "        return 'POST'\n"
                "def fetch(url):\n"
                "    return urlopen(UnsafeRequest(url))\n"
            ),
            "opener body": (
                "import urllib.request as url_request\n"
                "OPENER = url_request.build_opener()\n"
                "def fetch(url, payload):\n"
                "    return OPENER.open(url, data=payload)\n"
            ),
            "prebuilt request": (
                "from urllib.request import urlopen\n"
                "def fetch(request):\n"
                "    return urlopen(request)\n"
            ),
        }
        for case, source in fixtures.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                package = Path(directory) / "stock_monitor"
                provider = package / "providers" / "http.py"
                provider.parent.mkdir(parents=True)
                provider.write_text(source, encoding="utf-8")

                violations = _boundary_violations(package)

                self.assertTrue(
                    any(
                        violation.startswith("network capability:")
                        for violation in violations
                    )
                )

    def test_raw_network_primitives_cannot_escape_designated_provider(self) -> None:
        consumers = {
            "named import": (
                "from stock_monitor.providers.http import URLRequest, open_url\n"
                "def fetch(url):\n"
                "    return open_url(URLRequest(url))\n"
            ),
            "star import": (
                "from stock_monitor.providers.http import *\n"
                "def fetch(url):\n"
                "    return open_url(URLRequest(url))\n"
            ),
        }
        for case, source in consumers.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                package = Path(directory) / "stock_monitor"
                provider = package / "providers" / "http.py"
                provider.parent.mkdir(parents=True)
                provider.write_text(
                    "from urllib.request import Request as URLRequest\n"
                    "from urllib.request import urlopen as open_url\n",
                    encoding="utf-8",
                )
                (package / "consumer.py").write_text(source, encoding="utf-8")

                violations = _boundary_violations(package)

                self.assertTrue(
                    any(
                        violation.startswith("network capability:")
                        for violation in violations
                    )
                )

    def test_dynamic_network_imports_are_rejected(self) -> None:
        fixtures = {
            "dunder import": "transport = __import__('urllib.request')\n",
            "importlib": (
                "from importlib import import_module as load\n"
                "transport = load('requests')\n"
            ),
            "dynamic target": (
                "from importlib import import_module as load\n"
                "def choose(module_name):\n"
                "    return load(module_name)\n"
            ),
        }
        for case, source in fixtures.items():
            with self.subTest(case=case), tempfile.TemporaryDirectory() as directory:
                package = Path(directory) / "stock_monitor"
                package.mkdir()
                (package / "feature.py").write_text(source, encoding="utf-8")

                violations = _boundary_violations(package)

                self.assertTrue(
                    any(
                        violation.startswith("network capability:")
                        for violation in violations
                    )
                )


if __name__ == "__main__":
    unittest.main()
