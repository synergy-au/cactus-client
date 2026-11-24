from cactus_test_definitions import CSIPAusVersion
from cactus_test_definitions.server import TestProcedure, TestProcedureId, get_all_test_procedures, get_test_procedure


def get_test_procedure_supported_versions(tp: TestProcedure) -> list[CSIPAusVersion]:
    """Include the v1.3 beta storage version for procedures that target v1.3."""
    versions = list(tp.target_versions)
    if CSIPAusVersion.RELEASE_1_3 in versions and CSIPAusVersion.BETA_1_3_STORAGE not in versions:
        versions.append(CSIPAusVersion.BETA_1_3_STORAGE)
    return versions


def procedure_supports_version(tp: TestProcedure, version: CSIPAusVersion | str) -> bool:
    """Return whether a procedure supports a version, including repo-local version aliases."""
    return version in get_test_procedure_supported_versions(tp)


def get_test_procedures_for_version(version: CSIPAusVersion) -> list[tuple[TestProcedureId, TestProcedure]]:
    """Returns all TestProcedures for the specified CSIP-AUS version - will be sorted by TestProcedureId"""
    all_procedures = get_all_test_procedures()
    return sorted(
        (item for item in all_procedures.items() if procedure_supports_version(item[1], version)),
        key=lambda item: item[0],
    )


def get_test_procedure_for_version(version: CSIPAusVersion, test_procedure_id: TestProcedureId) -> TestProcedure | None:
    """Returns a specific TestProcedure by ID and ensures its compatible with the specified CSIP-AUS version"""

    try:
        tp = get_test_procedure(test_procedure_id)
    except Exception:
        return None

    if not procedure_supports_version(tp, version):
        return None

    return tp


def get_test_procedure_ids_for_version(version: CSIPAusVersion) -> set[TestProcedureId]:
    """Returns all TestProcedureIds for the specified CSIP-AUS version"""
    all_procedures = get_all_test_procedures()
    return set(tp_id for tp_id, tp in all_procedures.items() if procedure_supports_version(tp, version))
