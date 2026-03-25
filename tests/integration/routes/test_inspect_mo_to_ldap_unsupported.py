import pytest
from httpx import AsyncClient


@pytest.mark.integration_test
@pytest.mark.envvar({"LISTEN_TO_CHANGES_IN_MO": "False"})
async def test_inspect_mo_to_ldap_unsupported_type(test_client: AsyncClient) -> None:
    response = await test_client.get("/Inspect/mo_to_ldap/address/all")
    assert response.status_code == 404
    assert response.json()["detail"] == "Unsupported type: address"
