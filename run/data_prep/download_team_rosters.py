from pathlib import Path
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
        capture_output = False,
        check = True,
    )

if __name__ == "__main__":
    print("Downloading Team Rosters...")
    # Script Directory Path
    base_path = Path(__file__).resolve().parent
    print(base_path)
    
    # Run Path
    run_dir = base_path.parent 
    print(run_dir)
    
    # Rscript path
    get_team_rosters_path = run_dir / "R/get_team_rosters_run.r"
    print(get_team_rosters_path)

    download_team_rosters(get_team_rosters_run_r_path = get_team_rosters_path)
    print("Downloaded Team Rosters. data was saved to the data directory of the project")
