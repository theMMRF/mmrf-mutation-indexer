import asyncio
import itertools
import logging
import pathlib
from collections.abc import Iterable, Sequence

import elasticsearch
import halo
import more_itertools
import tap

from mutation_indexer import configuration, gene_expression, project_visibility, viz
from mutation_indexer.configuration import build
from mutation_indexer.constants import app

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


class Args(tap.Tap):
    driver: app.Driver
    config: Sequence[pathlib.Path]

    def configure(self) -> None:
        self.add_argument(
            "driver",
            type=app.Driver,
            help="Which driver should be ran by the client.",
        )
        self.add_argument(
            "config",
            type=pathlib.Path,
            nargs="*",
            help="Any user configuration files which should be loaded.",
        )


def get_file_args(config: build.Build) -> Iterable[tuple[str, str]]:
    """
    Sets the spark-submit config values as well as jars params.

    Yields:
        a tuple of argument flag and value.
    """
    inputs = project_visibility.submission_inputs()
    files = [
        f"{config.config_file}#{app.CONFIGURATION_FILE}",
        f"{config.pex_file}#{app.PEX_FILE}",
    ]
    files.extend(f"{uri}#{alias}" for _, uri, alias in inputs)
    yield ("--conf", f"spark.yarn.dist.files={','.join(files)}")
    if inputs:
        yield ("--conf", "spark.yarn.appMasterEnv.PROJECT_VISIBILITY_ENABLED=true")
        for variable, _, alias in inputs:
            yield ("--conf", f"spark.yarn.appMasterEnv.{variable}=./{alias}")
    yield (
        "--jars",
        ",".join(map(str, config.jar_dir.glob("**/*.jar"))),
    )


async def run_spark_command(config: configuration.Configuration, driver: app.Driver) -> None:
    """
    Runs the spark-submit command which will spwan the spark application. The spark
    application will build the desired indices.

    Args:
        config: the configuration for the build.
    """
    config_arguments = config.spark.get_arguments()
    file_arguments = get_file_args(config.build)
    arguments = more_itertools.flatten(itertools.chain(config_arguments, file_arguments))
    final_command = " ".join(
        more_itertools.value_chain(
            str(config.build.spark_submit),
            arguments,
            str(config.build.driver),
            driver.value,
        )
    )

    with (
        open(config.build.output_log, "wb+") as out_file,
        open(config.build.error_log, "wb+") as error_file,
    ):
        process = await asyncio.create_subprocess_shell(
            final_command, stdout=out_file, stderr=error_file
        )

        await process.wait()
        if process.returncode != 0:
            raise RuntimeError(f"spark-submit failed with exit code {process.returncode}; inspect the configured build logs")


async def force_merge_indices(config: configuration.Configuration) -> None:
    """
    Performs a force merge on the indices that have been created.

    Args:
        config: The configuration with which the build was run.
    """
    async with elasticsearch.AsyncElasticsearch(
        config.elasticsearch.connection.nodes.split(","),
        use_ssl=config.elasticsearch.connection.use_ssl,
        verify_certs=config.elasticsearch.connection.verify_certs,
        http_auth=(
            config.elasticsearch.connection.user,
            config.elasticsearch.connection.password,
        ),
    ) as es_client:
        indices = [config.elasticsearch.write.indices[i] for i in config.build.index_types]

        try:
            await es_client.indices.forcemerge(
                index=indices, max_num_segments=1, ignore_unavailable=True
            )
        except Exception as ex:
            logger.warning(f"Error occurred while merging: {ex}.")


async def _main(args: Args) -> None:
    if args.driver == app.Driver.GENE_EXPRESSION:
        configuration_class = gene_expression.Configuration
    elif args.driver == app.Driver.VIZ:
        configuration_class = viz.Configuration
    else:
        raise ValueError(f"Unknown driver: {args.driver}")

    with configuration_class.client_context(args.config) as config:
        print(f"RUNNING BUILD: {config.build.build_id}")

        with halo.Halo(spinner="pong") as spinner:
            try:
                spinner.text = "Running spark-submit"
                await run_spark_command(config, args.driver)
                spinner.text = "Merging indices"
                await force_merge_indices(config)
            except:
                spinner.fail("Process Failed")
                raise
            else:
                spinner.succeed("Indices built")


def main() -> None:
    try:
        args = Args().parse_args()

        asyncio.run(_main(args))
    except:  # noqa: E722
        logger.critical("Application failed.", exc_info=True)
        raise


if __name__ == "__main__":
    main()
