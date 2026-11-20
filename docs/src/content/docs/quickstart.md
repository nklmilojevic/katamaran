---
title: Quickstart tutorial
description: Start the VM, run a Docker container with a shared folder and port, and launch a Linux machine.
---

In this tutorial, you start the katamaran VM, serve a file from your Mac inside a container, verify live file changes, and open a shell inside an Ubuntu Linux machine.

Make sure you have completed [Install katamaran](/katamaran/install/) before starting.

## 1. Start the VM

Start the VM and wait until Docker reports ready:

```sh
kat start
```

You should see confirmation that the daemon and Docker are ready. Both `kat start` and launching **Katamaran** set the active Docker context to `katamaran` automatically. Check the VM state:

```sh
kat status
```

The output shows the VM state, CPU count, memory limit, Docker readiness, `~/Katamaran` mount status, and forwarded ports.

## 2. Run a container with a bind mount

Create a scratch directory on your Mac with a sample file:

```sh
mkdir -p ~/katamaran-demo
printf 'hello from macos\n' > ~/katamaran-demo/index.html
```

Start an Alpine container that binds `~/katamaran-demo` at `/site` and serves it on port `8080` without a `-p` flag:

```sh
docker run -d --rm --name demo-web -v ~/katamaran-demo:/site alpine:3.24 sh -c 'cd /site && exec httpd -f -p 8080'
```

Because automatic port forwarding is enabled by default, request the file from your Mac at `127.0.0.1:8080`:

```sh
curl http://127.0.0.1:8080/index.html
```

You should see:

```text
hello from macos
```

Edit the file on your Mac and request it again:

```sh
printf 'updated on macos\n' > ~/katamaran-demo/index.html
curl http://127.0.0.1:8080/index.html
```

You should see `updated on macos` immediately. macOS FSEvents invalidates the guest's cached file metadata and data on every host change.

Stop the container and remove the scratch directory:

```sh
docker rm -f demo-web
rm -rf ~/katamaran-demo
```

## 3. Create a Linux machine

Create an Ubuntu machine named `dev`:

```sh
kat machine create dev
```

Wait for the image download and first boot to finish. Open an interactive shell in `dev`:

```sh
kat shell dev
```

You are logged in as a user with your macOS username. Check the OS release, then exit the shell:

```sh
cat /etc/os-release
exit
```

List your machines to confirm `dev` is running:

```sh
kat machine list
```

If you no longer need `dev`, delete it:

```sh
kat machine delete dev
```

## Next steps

- Read [Use Docker](/katamaran/docker/) for Compose, storage, and socket details.
- Read [File sharing](/katamaran/file-sharing/) to inspect Linux files in Finder and tune bind mounts.
- Read [Configuration reference](/katamaran/configuration/) to adjust CPU, memory, and sleep settings.
