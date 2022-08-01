# Copyright 2021 Canonical Ltd.
# See LICENSE file for licensing details.

import json
import logging
from types import SimpleNamespace
from typing import Dict, List, Set

from pytest_operator.plugin import OpsTest

logger = logging.getLogger(__name__)


class StorageAnalyzer:
    def __init__(self, ops_test: OpsTest):
        self.ops_test = ops_test
        self._states = []

    async def run(self) -> SimpleNamespace:
        """Returns the info of the storage state of the latest juju unit.

        Returns:
            SimpleNamespace(
                active_entities=[{
                    juju_unit_id="mongodb-k8s/0",
                    k8s_pod_id="mongodb-k8s-0",
                    k8s_attached_pvc_ids=["pvc-123"]
                }],
                orphan_volumes=["pvc-234"]
            )
        """
        status_resp = await self.ops_test.juju(
            "status", f"--model={self.ops_test.model_name}", "--format=json"
        )

        status = json.loads(status_resp[1])

        storage_state = self._load_storage_state(status)
        self._states.append(storage_state)

        return storage_state

    @staticmethod
    def _load_storage_state(juju_status_resp: dict) -> SimpleNamespace:
        """Parses the json status output into a SimpleNamespace."""
        active_entities = []

        juju_units = juju_status_resp["applications"]["mongodb-k8s"]["units"]
        for juju_unit_id, juju_unit in juju_units.items():
            active_entity = SimpleNamespace(
                juju_unit_id=juju_unit_id,
                k8s_pod_id=juju_unit["provider-id"],
                k8s_attached_pvc_ids=set(),
            )

            for volume_index, volume in juju_status_resp["storage"]["volumes"].items():
                if "attachments" not in volume:
                    continue

                if juju_unit_id not in volume["attachments"]["units"]:
                    continue

                if volume["attachments"]["units"][juju_unit_id].get("life", None) != "alive":
                    continue

                active_entity.k8s_attached_pvc_ids.add(volume["provider-id"])

            active_entities.append(active_entity)

        orphan_volumes = []
        for volume_index, volume in juju_status_resp["storage"]["volumes"].items():
            if volume["status"]["current"] == "detached":
                orphan_volumes.append(volume["provider-id"])

        return SimpleNamespace(active_entities=active_entities, orphan_volumes=orphan_volumes)

    def volumes_created(self) -> List[str]:
        """Returns list of distinct PVCs created up to now (all previous "run" calls)."""
        volumes = set()

        for storage_state in self._states:
            # volumes.update(storage_state.orphan_volumes)

            for entry in storage_state.active_entities:
                volumes.update(entry.k8s_attached_pvc_ids)

        return sorted(list(volumes))

    def units_created(self) -> List[str]:
        """Returns list of distinct juju units created up to now (all previous "run" calls)."""
        units = set()

        for storage_state in self._states:
            units.update([active.juju_unit_id for active in storage_state.active_entities])

        return sorted(list(units))

    def unit_to_pvcs(self) -> Dict[str, Set[str]]:
        """Returns a map of the distinct k8s pvcs that were attached to each juju unit.

        juju_unit_id_1 => {pvc_id_1}
        juju_unit_id_2 => {pvc_id_3}
        """
        attachment_log = {}
        for storage_state in self._states:

            for entry in storage_state.active_entities:
                if entry.juju_unit_id not in attachment_log:
                    attachment_log[entry.juju_unit_id] = set()

                attachment_log[entry.juju_unit_id].add(
                    *[pvc for pvc in entry.k8s_attached_pvc_ids]
                )

        return attachment_log

    def pvc_to_units(self):
        """Returns a map of the distinct juju units to which a PVC was  attached.

        pvc_id_1 => {juju_unit_id_1}
        pvc_id_2 => {juju_unit_id_2}
        """
        attachment_log = {}
        for storage_state in self._states:

            for entry in storage_state.active_entities:

                for k8s_attached_pvc_id in entry.k8s_attached_pvc_ids:
                    if k8s_attached_pvc_id not in attachment_log:
                        attachment_log[k8s_attached_pvc_id] = set()

                    attachment_log[k8s_attached_pvc_id].add(entry.juju_unit_id)

        return attachment_log
