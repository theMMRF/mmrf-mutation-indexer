import abc
import contextlib
import graphlib
import logging
import pathlib
from collections.abc import Iterable, Iterator

import elasticsearch
from indexclient import client
from pyspark import sql

from mutation_indexer import builders, configuration
from mutation_indexer import logging as mutation_indexer_logging
from mutation_indexer.configuration import elasticsearch as es_config
from mutation_indexer.configuration import indexd
from mutation_indexer.constants import app

logger = logging.getLogger("mutation_indexer")


@contextlib.contextmanager
def _initialize_spark() -> Iterator[sql.SparkSession]:
    """
    Initializes the spark session.

    Returns:
        The context manager for spark session for the current driver.
    """
    with sql.SparkSession.builder.config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.auth.AssumedRoleCredentialProvider"
    ) \
    .config(
        "spark.hadoop.fs.s3a.assumed.role.arn",
        "arn:aws:iam::006459778784:role/Gen3_EC2_r7i_KMS_Role"
    ) \
    .config(
        "spark.hadoop.fs.s3a.assumed.role.credentials.provider",
        "com.amazonaws.auth.InstanceProfileCredentialsProvider"
    ) \
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        "s3.us-east-1.amazonaws.com"
    ).getOrCreate() as spark_session:
        spark_session.sparkContext.setLogLevel("FATAL")

        yield spark_session


def get_index_client(config: indexd.IndexD) -> client.IndexClient:
    """
    Builds the index client with the given configuration values.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An indexd client
    """
    return client.IndexClient(
        # baseurl=f"{config.host}:{config.port}",
        baseurl=f"{config.host}",
        auth=(config.user, config.password),
    )


def get_es_client(config: es_config.Connection) -> elasticsearch.Elasticsearch:
    """
    builds the elastic search client based on the configuration.

    Args:
        config: The connection configuration for setting up the client.

    Returns:
        An elasticsearch client
    """

    return elasticsearch.Elasticsearch(
        config.nodes.split(","),
        use_ssl=config.use_ssl,
        verify_certs=config.verify_certs,
        http_auth=(config.user, config.password),
    )


class Driver[TConfig: configuration.Configuration](abc.ABC):
    """A class representing a driver which ultimately builds data using builders."""

    @classmethod
    @abc.abstractmethod
    def load_config(cls, file: pathlib.Path) -> TConfig:
        """Load the configuration associated with the driver from the file.

        Args:
            file: The file path to the configuration data.

        Returns:
            An instance of the loaded configuration.
        """
        ...

    @abc.abstractmethod
    def _initialize_builders(
        self, config: TConfig, spark_session: sql.SparkSession
    ) -> contextlib.AbstractContextManager[Iterable[builders.Builder]]:
        """Initialize all builders required for this run of the driver.

        NOTE: This is wrapped in a context manager in order to allow drivers to clean up
        any dependencies that the driver depends on after their use.

        Returns:
            A context manager wrapping an iterable of all required builders.
        """
        ...

    def _sort_builders(
        self, builders: Iterable[builders.Builder]
    ) -> Iterable[builders.Builder]:
        """Sorts the builders into a topological order based on their required inputs.

        Args:
            builders: The builders which need to be sorted in order to run them.

        Returns:
            The input builders in a topological order so that all required inputs will
            be build prior to a dependent builder being called.
        """
        builder_by_output = {b.output: b for b in builders}
        graph = graphlib.TopologicalSorter({b.output: b.inputs for b in builders})

        return tuple(builder_by_output[d] for d in graph.static_order())

    def run(self, config: TConfig) -> None:
        """Runs the driving executing the build of the expected data.

        Args:
            config: The configuration for this run of the driver.
        """
        inputs: dict[str, sql.DataFrame] = {}

        with (
            _initialize_spark() as spark_session,
            self._initialize_builders(config, spark_session) as builders,
        ):
            for builder in self._sort_builders(builders):
                spark_session.sparkContext.setJobGroup(
                    builder.output.name, f"Building {builder.output.name}"
                )

                inputs[builder.output.to_param()] = builder.build(**inputs)


def main[TConfig: configuration.Configuration](driver: Driver[TConfig]) -> None:
    """A main function for executing a driver run."""
    mutation_indexer_logging.configure()

    try:
        config = driver.load_config(pathlib.Path(app.CONFIGURATION_FILE))

        mutation_indexer_logging.add_build_id(config.build.build_id)
        driver.run(config)
    except Exception:
        logger.critical("Driver failed", exc_info=True)
