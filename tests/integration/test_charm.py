#!/usr/bin/env python3
# Copyright 2022 Canonical Ltd.
# See LICENSE file for licensing details.
import json
import logging
import time

import pytest
from helpers import (
    APP_NAME,
    METADATA,
    TEST_DOCUMENTS,
    UNIT_IDS,
    check_if_test_documents_stored,
    generate_collection_id,
    get_address_of_unit,
    get_latest_unit_id,
    get_leader_id,
    mongodb_uri,
    primary_host,
    run_mongo_op,
    secondary_mongo_uris_with_sync_delay,
)
from lightkube import AsyncClient
from lightkube.resources.core_v1 import Pod
from pymongo import MongoClient
from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


@pytest.mark.abort_on_fail
async def test_build_and_deploy(ops_test: OpsTest):
    """Build the charm-under-test and deploy it together with related charms.

    Assert on the unit status before any relations/configurations take place.
    """
    # build and deploy charm from local source folder
    charm = await ops_test.build_charm(".")
    resources = {"mongodb-image": METADATA["resources"]["mongodb-image"]["upstream-source"]}
    await ops_test.model.deploy(
        charm, resources=resources, application_name=APP_NAME, num_units=len(UNIT_IDS)
    )

    # issuing dummy update_status just to trigger an event
    await ops_test.model.set_config({"update-status-hook-interval": "10s"})

    await ops_test.model.wait_for_idle(
        apps=[APP_NAME],
        status="active",
        raise_on_blocked=True,
        timeout=1000,
    )
    assert ops_test.model.applications[APP_NAME].units[0].workload_status == "active"

    # effectively disable the update status from firing
    await ops_test.model.set_config({"update-status-hook-interval": "60m"})


@pytest.mark.abort_on_fail
@pytest.mark.parametrize("unit_id", UNIT_IDS)
async def test_application_is_up(ops_test: OpsTest, unit_id: int):
    address = await get_address_of_unit(ops_test, unit_id=unit_id)
    response = MongoClient(address, directConnection=True).admin.command("ping")
    assert response["ok"] == 1


async def test_application_primary(ops_test: OpsTest):
    """Tests existence of primary and verifies the application is running as a replica set.

    By retrieving information about the primary this test inherently tests password retrieval.
    """
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    primary = [
        member["name"] for member in rs_status.data["members"] if member["stateStr"] == "PRIMARY"
    ][0]

    assert primary, "mongod has no primary on deployment"

    number_of_primaries = 0
    for member in rs_status.data["members"]:
        if member["stateStr"] == "PRIMARY":
            number_of_primaries += 1

    assert number_of_primaries == 1, "more than one primary in replica set"

    leader_id = await get_leader_id(ops_test)
    assert (
        primary == f"mongodb-k8s-{leader_id}.mongodb-k8s-endpoints:27017"
    ), "primary not leader on deployment"


async def test_scale_up(ops_test: OpsTest):
    """Tests juju add-unit functionality.

    Verifies that when a new unit is added to the MongoDB application that it is added to the
    MongoDB replica set configuration.
    """
    # add two units and wait for idle
    await ops_test.model.applications[APP_NAME].scale(scale_change=2)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, wait_for_exact_units=5
    )
    num_units = len(ops_test.model.applications[APP_NAME].units)
    assert num_units == 5

    # grab juju hosts
    juju_hosts = [
        f"mongodb-k8s-{unit_id}.mongodb-k8s-endpoints:27017" for unit_id in range(num_units)
    ]

    # connect to replica set uri and get replica set members
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    mongodb_hosts = [member["name"] for member in rs_status.data["members"]]

    # verify that the replica set members have the correct units
    assert set(mongodb_hosts) == set(juju_hosts), (
        "hosts for mongodb: "
        + str(set(mongodb_hosts))
        + " and juju: "
        + str(set(juju_hosts))
        + " don't match"
    )


async def test_scale_down(ops_test: OpsTest):
    """Tests juju remove-unit functionality.

    This test verifies:
    1. multiple units can be removed while still maintaining a majority (ie remove a minority)
    2. Replica set hosts are properly updated on unit removal
    """
    # add two units and wait for idle
    await ops_test.model.applications[APP_NAME].scale(scale_change=-2)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, wait_for_exact_units=3
    )
    num_units = len(ops_test.model.applications[APP_NAME].units)
    assert num_units == 3

    # grab juju hosts
    juju_hosts = [
        f"mongodb-k8s-{unit_id}.mongodb-k8s-endpoints:27017" for unit_id in range(num_units)
    ]

    # connect to replica set uri and get replica set members
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    mongodb_hosts = [member["name"] for member in rs_status.data["members"]]

    # verify that the replica set members have the correct units
    assert set(mongodb_hosts) == set(juju_hosts), (
        "hosts for mongodb: "
        + str(set(mongodb_hosts))
        + " and juju: "
        + str(set(juju_hosts))
        + " don't match"
    )

    # verify that the set maintains a primary
    primary = [
        member["name"] for member in rs_status.data["members"] if member["stateStr"] == "PRIMARY"
    ][0]

    assert primary in juju_hosts, "no primary after scaling down"


async def test_replication_primary_reelection(ops_test: OpsTest):
    """Tests removal of Mongodb primary and the reelection functionality.

    Verifies that after the primary server gets removed,
    a successful reelection happens.
    """
    # retrieve the status of the replica set
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    # get the primary host from the rs_status response
    primary = primary_host(rs_status.data)
    assert primary, "no primary set"

    replica_name = primary.split(".")[0]

    # Deleting the primary pod using kubectl
    k8s_client = AsyncClient(namespace=ops_test.model_name)
    await k8s_client.delete(Pod, name=replica_name)

    # the median time in which a reelection event happens is after around 12 seconds
    # setting the double to be on the safe side
    time.sleep(24)

    # retrieve the status of the replica set
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    # get the new primary host after reelection
    new_primary = primary_host(rs_status.data)
    assert new_primary, "no new primary set"
    assert new_primary != primary


async def test_replication_data_consistency(ops_test: OpsTest):
    """Test the data consistency between the primary and secondaries.

    Verifies that after writing data to the primary the data on
    the secondaries match.
    """
    # generate a collection id
    collection_id = generate_collection_id()

    # Create a database and a collection (lazily)
    create_collection = await run_mongo_op(
        ops_test, f'db.createCollection("{collection_id}")', suffix=f"?replicaSet={APP_NAME}"
    )
    assert create_collection.succeeded and create_collection.data["ok"] == 1

    # Store a few test documents
    insert_many_docs = await run_mongo_op(
        ops_test,
        f"db.{collection_id}.insertMany({TEST_DOCUMENTS})",
        suffix=f"?replicaSet={APP_NAME}",
    )
    assert insert_many_docs.succeeded and len(insert_many_docs.data["insertedIds"]) == 2

    # attempt ensuring that the replication happened on all secondaries
    time.sleep(24)

    # query the primary only
    set_primary_read_pref = await run_mongo_op(
        ops_test,
        'db.getMongo().setReadPref("primary")',
        suffix=f"?replicaSet={APP_NAME}",
        expecting_output=False,
    )
    assert set_primary_read_pref.succeeded
    await check_if_test_documents_stored(ops_test, collection_id)

    # query the secondaries with the pymongo default behavior: majority
    set_secondary_read_pref = await run_mongo_op(
        ops_test,
        'db.getMongo().setReadPref("secondary")',
        suffix=f"?replicaSet={APP_NAME}",
        expecting_output=False,
    )
    assert set_secondary_read_pref.succeeded
    await check_if_test_documents_stored(ops_test, collection_id)

    # query the secondaries by targeting units
    rs_status = await run_mongo_op(ops_test, "rs.status()")
    assert rs_status.succeeded, "mongod had no response for 'rs.status()'"

    secondaries = await secondary_mongo_uris_with_sync_delay(ops_test, rs_status.data)

    # verify that each secondary contains the data
    synced_secondaries_count = 0
    for secondary in secondaries:
        time.sleep(secondary["delay"] + 2)  # probably useless, but attempting
        try:
            await check_if_test_documents_stored(
                ops_test, collection_id, mongo_uri=secondary["uri"]
            )
        except Exception:
            # there may need some time to finish replicating to this specific secondary
            continue

        synced_secondaries_count += 1

    logger.info(
        f"{synced_secondaries_count}/{len(secondaries)} secondaries fully synced with primary."
    )
    assert synced_secondaries_count > 0


async def test_replication_data_persistence_after_scaling(ops_test: OpsTest):
    """Test the data is not lost on scaling down.

    Verifies that after scaling up, the data is replicated to the new secondary
    and that on scaling down the data is not lost.
    """
    # generate a collection id
    collection_id = generate_collection_id()

    # Create a database and a collection (lazily)
    create_collection = await run_mongo_op(
        ops_test, f'db.createCollection("{collection_id}")', suffix=f"?replicaSet={APP_NAME}"
    )
    assert create_collection.succeeded and create_collection.data["ok"] == 1

    # add one unit and wait for idle
    await ops_test.model.applications[APP_NAME].scale(scale_change=1)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=1000, wait_for_exact_units=4
    )

    num_units = len(ops_test.model.applications[APP_NAME].units)
    assert num_units == 4

    # Store a few test documents
    insert_many_docs = await run_mongo_op(
        ops_test,
        f"db.{collection_id}.insertMany({TEST_DOCUMENTS})",
        suffix=f"?replicaSet={APP_NAME}",
    )
    assert insert_many_docs.succeeded and len(insert_many_docs.data["insertedIds"]) == 2

    # attempt ensuring that the replication happened on all secondaries
    time.sleep(24)

    # query the secondaries by targeting units
    # choosing the 3rd unit, going with the assumption that juju downscales
    # from the higher unit downwards
    latest_secondary_mongo_uri = await mongodb_uri(ops_test, [get_latest_unit_id(ops_test)])
    await check_if_test_documents_stored(
        ops_test, collection_id, mongo_uri=latest_secondary_mongo_uri
    )

    # get k8s_volume_id of the unit with ID: 3
    storage_resp = await ops_test.juju("list-storage", "--format=json")
    storage = json.loads(storage_resp[1])
    latest_unit_id = str(get_latest_unit_id(ops_test))
    k8s_volume_id = storage["volumes"][latest_unit_id]["provider-id"]

    # scale down
    await ops_test.model.applications[APP_NAME].scale(scale_change=-1)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=3000, wait_for_exact_units=3
    )
    num_units = len(ops_test.model.applications[APP_NAME].units)
    assert num_units == 3

    # scale back up by 1 unit
    await ops_test.model.applications[APP_NAME].scale(scale_change=1)
    await ops_test.model.wait_for_idle(
        apps=[APP_NAME], status="active", timeout=3000, wait_for_exact_units=4
    )
    num_units = len(ops_test.model.applications[APP_NAME].units)
    assert num_units == 4

    # check if k8s is reusing the previous volume from before scale down
    storage_resp = await ops_test.juju("list-storage", "--format=json")
    storage = json.loads(storage_resp[1])

    latest_unit_id = str(get_latest_unit_id(ops_test))
    new_k8s_volume_id = storage["volumes"][latest_unit_id]["provider-id"]

    assert k8s_volume_id == new_k8s_volume_id

    # check if the old data is still there
    latest_secondary_mongo_uri = await mongodb_uri(ops_test, [get_latest_unit_id(ops_test)])
    await check_if_test_documents_stored(
        ops_test, collection_id, mongo_uri=latest_secondary_mongo_uri
    )
