import argparse
import concurrent.futures as cf
import datetime

import aind_session
import codeocean.computation
import codeocean.data_asset
import npc_lims

argparser = argparse.ArgumentParser(description="Run capsules for a given session raw data asset")
argparser.add_argument("--raw_data_asset_id", type=str, required=True, help="ID of the raw data asset to process")
args = argparser.parse_args()
raw_data_asset_id = args.raw_data_asset_id

CAPSULE_ID = {
    'gamma_encoding': 'd8fa238e-1e53-4890-af5e-f1479c1551fc',
    'lp_face_parts': '92b943ad-b231-40d2-b580-5cc7945e617f',
    'dlc_eye': '4cf0be83-2245-4bb1-a55c-a78201b14bfe',
    'facemap': '670de0b3-f73d-4d22-afe6-6449c45fada4',
}

client = aind_session.get_codeocean_client()
raw_data_asset = client.data_assets.get_data_asset(raw_data_asset_id)
session_id = raw_data_asset.name
print(session_id)

def run_and_capture_result(process_name: str, capsule_id: str, data_asset_ids: list[str]) -> codeocean.data_asset.DataAsset:
    run_capsule_request = codeocean.computation.RunParams(
        capsule_id=capsule_id,
        data_assets=[
            codeocean.computation.DataAssetsRunParam(id=asset_id, mount=session_id)
            for asset_id in data_asset_ids
        ],
    )
    # launch capsule:
    fname = f"/root/capsule/results/run_{process_name}.json"
    print(f"Launching {process_name} with params in {fname}")
    with open(fname, "w") as f:
        f.write(run_capsule_request.to_json(indent=4))
    computation: codeocean.computation.Computation = client.computations.run_capsule(run_capsule_request)

    # wait for capsule to complete:
    computation: codeocean.computation.Computation = client.computations.wait_until_completed(computation, polling_interval=60, timeout=None)
    if npc_lims.is_computation_errored(computation):
        raise RuntimeError(f"Capsule {process_name} failed: {computation.id}")

    # capture result:
    created_dt = (
        datetime.datetime.fromtimestamp(computation.created)
        .isoformat(sep="_", timespec="seconds")
        .replace(":", "-")
    )
    data_asset_name = f"{session_id}_{process_name}_{created_dt}"
    subject_id = session_id.split('_')[1]
    custom_metadata = {
        "data level": "derived data",
        "experiment type": "ecephys",
        "modality": "Extracellular electrophysiology",
        "subject id": subject_id,
    }
    data_asset_params = codeocean.data_asset.DataAssetParams(
        name=data_asset_name,
        mount=data_asset_name,
        tags=[subject_id, "derived", "results"],
        source=codeocean.data_asset.Source(computation=codeocean.data_asset.ComputationSource(id=computation.id)),
        custom_metadata=custom_metadata,
    )
    fname = f"/root/capsule/results/capture_{process_name}.json"
    print(f"Creating data asset for {process_name} with params in {fname}")
    with open(fname, "w") as f:
        f.write(data_asset_params.to_json(indent=4))
    data_asset = client.data_assets.create_data_asset(data_asset_params)
    data_asset = aind_session.wait_until_ready(data_asset.id, timeout=None)
    npc_lims.set_asset_viewable_for_everyone(data_asset.id)
    print(f"Created data asset {process_name}: {data_asset.id}")
    return data_asset

def _post_gamma_encoding_callback(future: cf.Future[codeocean.data_asset.DataAsset]) -> None:
    result: codeocean.data_asset.DataAsset = future.result()
    print(f"Launching lp_face_parts with gamma encoding result {result.id}")
    run_and_capture_result('lp_face_parts', CAPSULE_ID['lp_face_parts'], [result.id])

def main() -> None:
    with cf.ThreadPoolExecutor() as executor:
        future_to_process_name = {}
        for process_name, capsule_id in CAPSULE_ID.items():
            if process_name == 'lp_face_parts':
                continue
            future = executor.submit(
                run_and_capture_result, process_name, capsule_id, [raw_data_asset_id]
            )
            if process_name == 'gamma_encoding':
                future.add_done_callback(_post_gamma_encoding_callback)
            future_to_process_name[future] = process_name
        for future in cf.as_completed(future_to_process_name):
            result = future.result()
            process_name = future_to_process_name[future]
            print(f"Completed {process_name}")

if __name__ == "__main__":
    main()
