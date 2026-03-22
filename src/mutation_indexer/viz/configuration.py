"""
For documentation concerning Mutation Indexer configuration please refer to the wiki
documentation @
https://gdc-ctds.atlassian.net/wiki/spaces/GDC/pages/76316689/Mutation+Indexer+Procedure#Configuration
"""

import dataclasses
from collections.abc import Iterable, Sequence
from importlib import abc, resources
from typing import Annotated

import marshmallow_dataclass
from marshmallow import fields

from mutation_indexer import configuration
from mutation_indexer.configuration import _extensions, builders, elasticsearch, indexd

# these are directly imported to created a better interface when using the viz module
from mutation_indexer.configuration.builders import GeneModelBuilder
from mutation_indexer.constants import app


# @dataclasses.dataclass(frozen=True)
# class ASCATBuilder(builders.Builder):
#     """
#     Configuration values for running the ASCAT builder
#     """

#     omit_cnv_data: bool


# @dataclasses.dataclass(frozen=True)
# class ASCATMetadataBuilder(builders.Builder):
#     """Configuration values for the ASCAT metadata builder."""

#     @dataclasses.dataclass(frozen=True)
#     class Priority:
#         experimental_strategy: str
#         workflow_type: str

#     priorities: Annotated[
#         Sequence[Priority],
#         _extensions.ArrayTupleField(
#             fields.Nested(marshmallow_dataclass.class_schema(Priority))
#         ),
#     ]


@dataclasses.dataclass(frozen=True)
class CaseBuilder(builders.Builder):
    """
    Configuration values for running the case builder
    """

    include_as_arrays: Annotated[Sequence[str], _extensions.ArrayTupleField(fields.String)]
    repartition_size: int


class CIVIC:
    @dataclasses.dataclass(frozen=True)
    class DNABuilder(builders.ResourceBuilder): ...

    @dataclasses.dataclass(frozen=True)
    class ProteinBuilder(builders.ResourceBuilder): ...


@dataclasses.dataclass(frozen=True)
class MAFBuilder(builders.Builder):
    """
    Configuration values for running the MAF builder
    """

    repartition_size: int


@dataclasses.dataclass(frozen=True)
class MAFMetadataBuilder(builders.Builder):
    """
    Configuration values for running the MAF metadata builder
    """

    prioritized_experimental_strategies: Annotated[
        Sequence[str], _extensions.ArrayTupleField(fields.String)
    ]


@dataclasses.dataclass(frozen=True)
class PrimaryAliquotBuilder(builders.Builder): ...


@dataclasses.dataclass(frozen=True)
class SegmentCNVBuilder(builders.Builder): ...


@dataclasses.dataclass(frozen=True)
class SegmentCNVMetadataBuilder(builders.Builder):
    """
    Configuration values for running the segment cnv metadata builder.
    """

    use_deprecated_query: bool


@dataclasses.dataclass(frozen=True)
class CaseCentricBuilder(builders.IndexBuilder):
    """
    Configuration values for running the case centric builder
    """

    genes_threshold: int
    include_as_arrays: Annotated[Sequence[str], _extensions.ArrayTupleField(fields.String)]


@dataclasses.dataclass(frozen=True)
class CNVCentricBuilder(builders.IndexBuilder):
    """
    Configuration values for running the cnv centric builder
    """

    occurrences_threshold: int


@dataclasses.dataclass(frozen=True)
class CNVOccurrenceCentricBuilder(builders.IndexBuilder): ...


@dataclasses.dataclass(frozen=True)
class GeneCentricBuilder(builders.IndexBuilder): ...


@dataclasses.dataclass(frozen=True)
class SegmentCNVCentricBuilder(builders.IndexBuilder):
    """
    Configuration values for running the segment cnv centric builder
    """


@dataclasses.dataclass(frozen=True)
class SegmentCNVOccurrenceCentricBuilder(builders.IndexBuilder):
    """
    Configuration values for running the segment cnv occurrence centric builder
    """


@dataclasses.dataclass(frozen=True)
class SSMCentricBuilder(builders.IndexBuilder):
    """
    Configuration values for running the SSM centric builder
    """

    occurrences_threshold: int


@dataclasses.dataclass(frozen=True)
class SSMOccurrenceCentricBuilder(builders.IndexBuilder): ...


@dataclasses.dataclass(frozen=True)
class GISTICBuilder(builders.Builder):
    """
    Configuration values for running the GISTIC builder
    """


@dataclasses.dataclass(frozen=True)
class GISTICMetadataBuilder(builders.Builder):
    """Configuration values for the GISTIC metadata builder."""

    @dataclasses.dataclass(frozen=True)
    class Priority:
        experimental_strategy: str
        workflow_type: str

    priorities: Annotated[
        Sequence[Priority],
        _extensions.ArrayTupleField(
            fields.Nested(marshmallow_dataclass.class_schema(Priority))
        ),
    ]


@dataclasses.dataclass(frozen=True)
class Builders:
    """
    Configuration values for running the export of the viz indices
    """

    # ascat: ASCATBuilder
    # ascat_metadata: ASCATMetadataBuilder
    case: CaseBuilder
    civic_dna: CIVIC.DNABuilder
    civic_protein: CIVIC.ProteinBuilder
    gene_model: GeneModelBuilder
    maf_metadata: MAFMetadataBuilder
    maf: MAFBuilder
    primary_aliquot: PrimaryAliquotBuilder
    segment_cnv: SegmentCNVBuilder
    segment_cnv_metadata: SegmentCNVMetadataBuilder
    case_centric: CaseCentricBuilder
    cnv_centric: CNVCentricBuilder
    cnv_occurrence_centric: CNVOccurrenceCentricBuilder
    gene_centric: GeneCentricBuilder
    segment_cnv_centric: SegmentCNVCentricBuilder
    segment_cnv_occurrence_centric: SegmentCNVOccurrenceCentricBuilder
    ssm_centric: SSMCentricBuilder
    ssm_occurrence_centric: SSMOccurrenceCentricBuilder
    gistic: GISTICBuilder
    gistic_metadata: GISTICMetadataBuilder


class Configuration(configuration.Configuration):
    builders: Builders
    elasticsearch: elasticsearch.Elasticsearch
    indexd: indexd.IndexD

    @classmethod
    def _default_files(cls) -> Iterable[abc.Traversable]:
        return (
            *super()._default_files(),
            resources.files(app.Driver.VIZ.module) / app.CONFIGURATION_FILE,
        )
