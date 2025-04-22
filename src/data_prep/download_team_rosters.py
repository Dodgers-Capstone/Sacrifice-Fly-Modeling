import subprocess

def download_team_rosters(get_team_rosters_run_r_path: str):
    """
    Uses baseballr to download the team rosters from 2021 to today and save is as a parquet in the 
    data directory of the project. To make changes to the dates go to get_team_rosters_run.r file 
    located in the run directory of the project.

    Args:
        get_team_rosters_run_r_path (str): Path to get_team_rosters_run.r
    Returns:
        None
    """
    subprocess.run(
        ["rscript", get_team_rosters_run_r_path],
        capture_output=false,
        check=true,
    )

if __name__ == "__main__":
    print("Downloading Team Rosters...")
    run_dir = os.path.abspath("../../run/data_prep")
    get_team_rosters_path = os.path.join(run_dir, "get_team_rosters_run.r")
    download_team_rosters(get_team_rosters_run_r_path = get_team_rosters_path)
    print("Downloaded Team Rosters. data was saved to the data directory of the project")

