import enum


class DataFrame(enum.IntEnum):
    # ASCAT = enum.auto()
    # ASCAT_METADATA = enum.auto()
    BINARY = enum.auto()
    CASE = enum.auto()
    CASE_CENTRIC = enum.auto()
    CASE_SQL = enum.auto()
    CIVIC_DNA = enum.auto()
    CIVIC_PROTEIN = enum.auto()
    CNV_CENTRIC = enum.auto()
    CNV_OCCURRENCE_CENTRIC = enum.auto()
    EXPRESSION_VALUE = enum.auto()
    GENE_CENTRIC = enum.auto()
    GENE_EXPRESSION = enum.auto()
    GENE_MODEL = enum.auto()
    GENE_SQL = enum.auto()
    MAF = enum.auto()
    MAF_METADATA = enum.auto()
    PRIMARY_ALIQUOT = enum.auto()
    SEGMENT_CNV = enum.auto()
    SEGMENT_CNV_CENTRIC = enum.auto()
    SEGMENT_CNV_METADATA = enum.auto()
    SEGMENT_CNV_OCCURRENCE_CENTRIC = enum.auto()
    SSM_CENTRIC = enum.auto()
    SSM_OCCURRENCE_CENTRIC = enum.auto()
    GISTIC = enum.auto()
    GISTIC_METADATA = enum.auto()

    def to_param(self) -> str:
        return f"{self.name}_df".lower()

    @classmethod
    def from_param(cls, param: str) -> "DataFrame":
        return cls[param[:-3].upper()]


class IndexType(enum.IntEnum):
    FILE = enum.auto()
    CASE = enum.auto()
    CASE_CENTRIC = enum.auto()
    CNV_CENTRIC = enum.auto()
    CNV_OCCURRENCE_CENTRIC = enum.auto()
    GENE_CENTRIC = enum.auto()
    SEGMENT_CNV_CENTRIC = enum.auto()
    SEGMENT_CNV_OCCURRENCE_CENTRIC = enum.auto()
    SSM_CENTRIC = enum.auto()
    SSM_OCCURRENCE_CENTRIC = enum.auto()
    GENE_EXPRESSION = enum.auto()

    def get_mappings_details(self) -> tuple[str, str | None]:
        if self in (IndexType.CASE, IndexType.FILE):
            return "gdc_from_graph", self.name.lower()

        return self.name.lower(), None


class BackupMode(enum.Enum):
    READ = enum.auto()
    WRITE = enum.auto()
    NEITHER = enum.auto()
    BOTH = enum.auto()

    def is_write(self) -> bool:
        return self == BackupMode.WRITE or self == BackupMode.BOTH

    def is_read(self) -> bool:
        return self == BackupMode.READ or self == BackupMode.BOTH
