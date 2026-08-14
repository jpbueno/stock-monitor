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
_NETWORK_MUTATION_VERBS = {"delete", "patch", "post", "put"}
_PROHIBITED_ENDPOINTS = (
    re.compile(r"/(?:v[0-9]+/)?orders(?:[/?#]|$)", re.IGNORECASE),
    re.compile(r"(?:^|[./])robinhood\.(?:com|net)(?:[:/]|$)", re.IGNORECASE),
)
_ALPACA_HOST = re.compile(
    r"(?<![a-z0-9-])(?P<host>(?:[a-z0-9-]+\.)+alpaca\.markets)"
    r"(?::[0-9]+)?(?=$|[/?#\s'\"])",
    re.IGNORECASE,
)
_APPROVED_ALPACA_DATA_HOST = "data.alpaca.markets"


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


def _call_name(call: ast.Call) -> str | None:
    if isinstance(call.func, ast.Name):
        return call.func.id
    if isinstance(call.func, ast.Attribute):
        return call.func.attr
    return None


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


def _is_network_mutation(node: ast.AST) -> bool:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value.strip().lower() in _NETWORK_MUTATION_VERBS
    if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
        return node.name.lower() in _NETWORK_MUTATION_VERBS
    if isinstance(node, ast.Attribute):
        return node.attr.lower() in _NETWORK_MUTATION_VERBS
    if isinstance(node, ast.Call):
        name = _call_name(node)
        if name is not None and name.lower() in _NETWORK_MUTATION_VERBS:
            return True
        if name is not None and name.lower() in {"request", "send", "urlopen"}:
            if any(
                isinstance(argument, ast.Constant)
                and isinstance(argument.value, str)
                and argument.value.lower() in _NETWORK_MUTATION_VERBS
                for argument in node.args
            ):
                return True
        return any(
            keyword.arg in {"method", "verb"}
            and isinstance(keyword.value, ast.Constant)
            and isinstance(keyword.value.value, str)
            and keyword.value.value.lower() in _NETWORK_MUTATION_VERBS
            for keyword in node.keywords
        )
    if isinstance(node, (ast.Assign, ast.AnnAssign)):
        value = node.value
        if not isinstance(value, ast.Constant) or not isinstance(value.value, str):
            return False
        if value.value.lower() not in _NETWORK_MUTATION_VERBS:
            return False
        targets = node.targets if isinstance(node, ast.Assign) else [node.target]
        return any(
            isinstance(target, ast.Name)
            and _identifier_tokens(target.id).intersection({"method", "verb"})
            for target in targets
        )
    return False


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
    for path in sorted(package.rglob("*.py")):
        relative = path.relative_to(package)
        module_tokens: set[str] = set()
        for segment in relative.with_suffix("").parts:
            module_tokens.update(_identifier_tokens(segment))
        if module_tokens.intersection(_FORBIDDEN_MODULE_TOKENS):
            violations.append(f"forbidden module:{relative}:0:module path")

        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(relative))
        docstrings = _docstring_constants(tree)
        for node in ast.walk(tree):
            line = getattr(node, "lineno", 0)
            if isinstance(node, (ast.Import, ast.ImportFrom)) and _has_forbidden_import(
                node
            ):
                violations.append(f"forbidden import:{relative}:{line}:import")
            if isinstance(node, (ast.ClassDef, ast.FunctionDef, ast.AsyncFunctionDef)):
                if _is_forbidden_api(node.name):
                    violations.append(
                        f"forbidden API:{relative}:{line}:{node.name}"
                    )
            if isinstance(node, ast.Attribute) and _is_forbidden_api(node.attr):
                violations.append(f"forbidden API:{relative}:{line}:{node.attr}")
            if isinstance(node, ast.Call):
                action = _brokerage_action(node)
                if action is not None:
                    violations.append(f"forbidden API:{relative}:{line}:{action}")
            if _is_network_mutation(node) and id(node) not in docstrings:
                violations.append(
                    f"network mutation:{relative}:{line}:non-GET capability"
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
                "network mutation",
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
                "TRADING_ORIGIN = 'https://broker-api.sandbox.alpaca.markets'\n"
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

    def test_boundary_scanner_rejects_indirect_mutation_shapes(self) -> None:
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
                "def mutate(client):\n"
                "    client.request('/market-data', method=HTTPMethod.POST)\n"
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
            any(violation.startswith("network mutation:") for violation in violations)
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


if __name__ == "__main__":
    unittest.main()
