import paramiko
import os
import stat

# --- SFTP Connection Parameters ---
HOSTNAME = '192.168.206.135'
PORT = 22
USERNAME = 'ldhullipalla'
PASSWORD = '!Password_1234!' # Use SSH keys for better security in production
LOCAL_DIR = r'C:\Users\ldhul\PycharmProjects\DE-Demo\datav2\batch' # Use 'r' prefix for raw string in Windows paths
REMOTE_DIR = '/home/ldhullipalla/data-v2/retail/batch' # Linux path format

def sftp_upload_multiple_files(hostname, port, username, password, local_dir, remote_dir):
    """
    Connects to an SFTP server and uploads multiple files from a local directory.
    """
    ssh_client = paramiko.SSHClient()
    # Automatically add the remote server's host key (use with caution, for simplicity here)
    ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    try:
        # Connect to the SFTP server
        ssh_client.connect(hostname, port, username, password)
        sftp_client = ssh_client.open_sftp()
        print(f"Connected to {hostname}")

        # Ensure the remote directory exists
        try:
            sftp_client.stat(remote_dir)
        except IOError:
            # Create remote directory if it does not exist
            sftp_client.mkdir(remote_dir)
            print(f"Created remote directory: {remote_dir}")

        # Iterate over files in the local directory
        for filename in os.listdir(local_dir):
            local_path = os.path.join(local_dir, filename)
            remote_path = os.path.join(remote_dir, filename).replace('\\', '/') # Ensure remote path uses Linux separators

            # Check if it is a file before uploading
            if os.path.isfile(local_path) and not stat.S_ISDIR(os.stat(local_path).st_mode):
                print(f"Uploading {filename}...")
                sftp_client.put(local_path, remote_path)
                print(f"Successfully uploaded {filename}")

        print("All files uploaded successfully!")

    except paramiko.AuthenticationException:
        print("Authentication failed, please check your credentials.")
    except paramiko.SSHException as e:
        print(f"SSH error: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        # Close the SFTP session and SSH connection
        if sftp_client:
            sftp_client.close()
        if ssh_client:
            ssh_client.close()
        print("Connection closed.")

# --- Run the function ---
sftp_upload_multiple_files(HOSTNAME, PORT, USERNAME, PASSWORD, LOCAL_DIR, REMOTE_DIR)
