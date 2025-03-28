#!/bin/bash

setup_ssh_free() {
    # Check if SSH is already passwordless
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "$USER@$HOSTNAME" true 2>/dev/null; then
        echo "Passwordless SSH is already set up for $USER@$HOSTNAME"
        return 0
    fi

    # Ensure SSH key exists
    if [ ! -f ~/.ssh/id_rsa ]; then
        echo "SSH key not found, generating new key..."
        ssh-keygen -t rsa -P '' -f ~/.ssh/id_rsa
    fi

    # Copy key to local machine for passwordless SSH
    echo "Setting up passwordless SSH..."
    ssh-copy-id "$USER@$HOSTNAME"

    # Verify if setup was successful
    if ssh -o BatchMode=yes -o ConnectTimeout=5 "$USER@$HOSTNAME" true 2>/dev/null; then
        echo "Passwordless SSH setup successful for $USER@$HOSTNAME"
        return 0
    else
        echo "Failed to set up passwordless SSH. Please check manually."
        return 1
    fi
}

setup_ssh_free