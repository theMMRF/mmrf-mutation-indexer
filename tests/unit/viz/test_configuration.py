from tests.integration.utils import test_setup


CGS_RISK_ARRAY_FIELDS = {
    "cgs_risk_key_criteria.cgs_risk_criteria",
    "cgs_risk_key_criteria.other_criteria",
}


def test_cgs_risk_fields_are_loaded_as_arrays() -> None:
    config = test_setup.load_viz_config()

    assert CGS_RISK_ARRAY_FIELDS <= set(config.builders.case.include_as_arrays)
    assert CGS_RISK_ARRAY_FIELDS <= set(
        config.builders.case_centric.include_as_arrays
    )
