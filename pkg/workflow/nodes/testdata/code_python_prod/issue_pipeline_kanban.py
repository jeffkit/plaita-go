def run(input):
    import subprocess
    try:
        r = subprocess.run(['python', '-m', 'issue_keeper', 'internal', 'move', input['repo_full'], str(input['issue_number']), 'review'], capture_output=True, text=True, timeout=60)
        return {'kanban_ok': r.returncode == 0}
    except Exception:
        return {'kanban_ok': False}
