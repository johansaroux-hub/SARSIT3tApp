import psutil
import os
import signal
import shutil
import datetime
import sys

def stop_flask_app(executable_name='app.exe'):
    """
    Finds and terminates the Flask app process by executable name.
    """
    terminated = False
    for proc in psutil.process_iter(['pid', 'name']):
        if proc.info['name'].lower() == executable_name.lower():
            print(f"Terminating Flask app with PID {proc.info['pid']}")
            try:
                os.kill(proc.info['pid'], signal.SIGTERM)
                terminated = True
            except Exception as e:
                print(f"Error terminating process: {e}")
    if not terminated:
        print(f"Flask app process '{executable_name}' not found.")

def backup_database(db_path='beneficial_ownership.db'):
    """
    Backs up the SQLite database to a timestamped file.
    """
    if not os.path.exists(db_path):
        print(f"Database file '{db_path}' not found. Backup aborted.")
        return
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    backup_path = f"backup_{timestamp}.db"
    try:
        shutil.copy(db_path, backup_path)
        print(f"Database backed up successfully to '{backup_path}'.")
    except Exception as e:
        print(f"Error backing up database: {e}")

if __name__ == '__main__':
    # Assume the script runs in the same directory as the database and executable
    os.chdir(os.path.dirname(os.path.abspath(__file__)))  # Set working directory to script location
    stop_flask_app('app.exe')  # Replace 'app.exe' with your Flask executable name
    backup_database('beneficial_ownership.db')
    input("Press Enter to exit...")  # Optional: Pause for user confirmation in console mode