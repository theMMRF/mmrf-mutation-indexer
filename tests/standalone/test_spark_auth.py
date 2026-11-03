"""Check session setup without installing Spark or contacting AWS.

Compile the session initializer in isolation; the rest of driver.py imports the
complete ETL stack, which is unnecessary for testing credential preservation.
"""

import ast
import contextlib
import unittest
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


class SparkAuthenticationTests(unittest.TestCase):
    def initializer(self, builder):
        source = Path(__file__).resolve().parents[2] / "src/mutation_indexer/driver.py"
        function = next(node for node in ast.parse(source.read_text()).body
                        if isinstance(node, ast.FunctionDef) and node.name == "_initialize_spark")
        namespace = {"contextlib": contextlib, "Iterator": Iterator,
                     "sql": SimpleNamespace(SparkSession=SimpleNamespace(builder=builder))}
        exec(compile(ast.Module(body=[function], type_ignores=[]), str(source), "exec"), namespace)
        return namespace["_initialize_spark"]

    def test_deployment_authentication_is_preserved(self):
        for provider in ("com.amazonaws.auth.InstanceProfileCredentialsProvider",
                         "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"):
            with self.subTest(provider=provider):
                builder = MagicMock()
                configured = {"spark.hadoop.fs.s3a.aws.credentials.provider": provider,
                              "spark.hadoop.fs.s3a.assumed.role.arn": "deployment-selected-role"}
                def override(key, value):
                    configured[key] = value
                    return builder
                builder.config.side_effect = override
                original = dict(configured)
                session = builder.getOrCreate.return_value.__enter__.return_value
                with self.initializer(builder)() as actual:
                    self.assertIs(actual, session)
                self.assertEqual(configured, original)
                builder.config.assert_not_called()
                session.sparkContext.setLogLevel.assert_called_once_with("FATAL")
                builder.getOrCreate.return_value.__exit__.assert_called_once()


if __name__ == "__main__":
    unittest.main()
