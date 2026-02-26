import argparse
import concurrent.futures as cf
import datetime

import aind_session
import codeocean.computation
import codeocean.data_asset

import npc_lims

argparser = argparse.ArgumentParser(description="Run capsules for a given session raw data asset")
argparser.add_argument("--raw_data_asset_id", type=str, required=True, help="ID of the raw data asset to process")
argparser.add_argument("--dry_run", type=int, default=0, help="Set to 1 for dry-run mode (no capsule launch)")
argparser.add_argument("--skip_existing", type=int, default=1, help="Set to 1 to skip existing data assets")
argparser.add_argument("--run_specific_capsule", type=str, default=0, help="Only run one of the names in CAPSULE_ID (sets skip_existing=False)")
argparser.add_argument("--skip_gamma_encoding", type=int, default=0, help="When re-running LPFaceParts, toggle this to use existing gamma encoding asset")

args = argparser.parse_args()
raw_data_asset_id = args.raw_data_asset_id
DRY_RUN = args.dry_run
if args.run_specific_capsule:
    print("Running specific capsule: will not skip if assets already exist")
    SKIP_EXISTING = False
else:
    SKIP_EXISTING = args.skip_existing

CAPSULE_ID = {
    'GammaEncoding': 'd8fa238e-1e53-4890-af5e-f1479c1551fc',
    'LPFaceParts': '92b943ad-b231-40d2-b580-5cc7945e617f',
    'dlc_eye': '4cf0be83-2245-4bb1-a55c-a78201b14bfe',
    'facemap': '670de0b3-f73d-4d22-afe6-6449c45fada4',
}


client = aind_session.get_codeocean_client()
raw_data_asset = client.data_assets.get_data_asset(raw_data_asset_id)
session_id = raw_data_asset.name
print(session_id)

def get_process_asset_id(process_name: str) -> str | None:
    try:
        return npc_lims.get_session_capsule_pipeline_data_asset(session_id, process_name)
    except FileNotFoundError:
        return None

def run_and_capture_result(process_name: str, data_asset_ids: list[str]) -> codeocean.data_asset.DataAsset | None:
    capsule_id = CAPSULE_ID[process_name]
    run_capsule_request = codeocean.computation.RunParams(
        capsule_id=capsule_id,
        data_assets=[
            codeocean.computation.DataAssetsRunParam(id=asset_id, mount=aind_session.get_codeocean_model(asset_id).name)
            for asset_id in data_asset_ids
        ],
    )
    fname = f"/root/capsule/results/run_{process_name}.json"
    with open(fname, "w") as f:
        f.write(run_capsule_request.to_json(indent=4))
    if DRY_RUN:
        print(f"\n[DRY RUN] Would launch capsule {process_name} with assets {data_asset_ids}")
        return None
    print(f"\nLaunching {process_name} with params in {fname}")
    computation: codeocean.computation.Computation = client.computations.run_capsule(run_capsule_request)
    computation: codeocean.computation.Computation = client.computations.wait_until_completed(computation, polling_interval=60, timeout=None)
    if npc_lims.is_computation_errored(computation):
        raise RuntimeError(f"Capsule {process_name} failed: {computation.id}")
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
    print(f"\nCreating data asset for {process_name} with params in {fname}")
    with open(fname, "w") as f:
        f.write(data_asset_params.to_json(indent=4))
    data_asset = client.data_assets.create_data_asset(data_asset_params)
    data_asset = aind_session.wait_until_ready(data_asset.id, timeout=None)
    npc_lims.set_asset_viewable_for_everyone(data_asset.id)
    print(f"\nCreated data asset {process_name}: {data_asset.id}")
    return data_asset

def _post_gamma_encoding_callback(future: cf.Future[codeocean.data_asset.DataAsset | None]) -> None:
    result = future.result()
    if result is None:
        print("\n[DRY RUN] Would launch LPFaceParts with gamma encoding result")
        return
    print(f"\nLaunching LPFaceParts with gamma encoding result {result.id}")
    run_and_capture_result('LPFaceParts', [result.id])

def main() -> None:
    
    if SKIP_EXISTING:
        existing_assets = {process_name: get_process_asset_id(process_name) for process_name in CAPSULE_ID.keys()}
        print(f"{SKIP_EXISTING=}, assets already exist for {[k for k,v in existing_assets.items() if v]}")
    else:
        print(f"{SKIP_EXISTING=}, will run all processes regardless of existing data assets")
        existing_assets = {}
    with cf.ThreadPoolExecutor() as executor:
        future_to_process_name = {}
        for process_name, capsule_id in CAPSULE_ID.items():
            if SKIP_EXISTING and existing_assets.get(process_name):
                print(f"\nSkipping {process_name}: asset already exists")
                continue
            if args.run_specific_capsule and process_name != args.run_specific_capsule:
                if args.run_specific_capsule == 'LPFaceParts' and process_name == 'GammaEncoding' and not args.skip_gamma_encoding:
                    pass # we requested LPFaceParts but GammaEncoding needs to run first
                else:
                    continue
            if process_name == 'LPFaceParts' and not (gamma_asset_id := existing_assets.get('GammaEncoding')) and not args.skip_gamma_encoding:
                # allow gamma encoding to run with done-callback attached below
                continue
            elif process_name == 'LPFaceParts' and gamma_asset_id:
                data_asset_id = gamma_asset_id
            else:
                data_asset_id = raw_data_asset_id
            future = executor.submit(
                run_and_capture_result, process_name, [data_asset_id]
            )
            if process_name == 'GammaEncoding':
                future.add_done_callback(_post_gamma_encoding_callback)
            future_to_process_name[future] = process_name
        for future in cf.as_completed(future_to_process_name):
            result = future.result()
            process_name = future_to_process_name[future]
            if result:
                print(f"\nCompleted {process_name}")

if __name__ == "__main__":
    main()
