---
title: Install katamaran
description: System requirements, installation from a disk image, first-time setup, and uninstall steps.
---

Install katamaran on an Apple silicon Mac and connect the Docker command-line tools to it.

## Requirements

- An Apple silicon Mac (M1 or later)
- macOS 15 or later
- The Docker command-line tools (`docker`, `docker-buildx`, and `docker-compose`)

Install the Docker command-line tools with Homebrew if you do not have them yet:

```sh
brew install docker docker-buildx docker-compose
```

If you plan to run `linux/amd64` images at native translation speed, install Apple Rosetta:

```sh
softwareupdate --install-rosetta
```

## Install with Homebrew

```sh
brew install --cask nklmilojevic/katamaran/katamaran
```

Open **Katamaran** from `/Applications` after Homebrew finishes.

## Install from the disk image

1. Download and open the latest signed [Katamaran.dmg](https://github.com/nklmilojevic/katamaran/releases/latest/download/Katamaran.dmg) (or browse all versions on [GitHub releases](https://github.com/nklmilojevic/katamaran/releases/latest)). To build the disk image yourself, follow [Build from source](/build-from-source/).
2. Drag `Katamaran.app` into `/Applications`.
3. Open **Katamaran** from `/Applications`.

On first launch (and after upgrades), **Katamaran** runs `katamaran setup --default` automatically. Setup installs:

- A LaunchAgent in `~/Library/LaunchAgents/` that runs the `katamaran daemon` process at login
- A Docker context named `katamaran` pointing to `unix://$HOME/.katamaran/run/docker.sock`, set as the default Docker context whenever Katamaran launches
- A symlink at `~/.local/bin/kat` pointing to the `katamaran` binary inside the app bundle

Add `~/.local/bin` to your shell `PATH` so you can run `kat` from any terminal.

## Run setup manually

To run setup from the command line, execute:

```sh
katamaran setup
```

To also make `katamaran` your default Docker context, pass `--default`:

```sh
katamaran setup --default
```

If you run a second isolated development instance with `KATAMARAN_HOME`, pass `--suffix` so its LaunchAgent, Docker context, and symlink do not collide with your main installation:

```sh
KATAMARAN_HOME=~/.katamaran-dev katamaran setup --suffix dev
```

## Verify the installation

Check that the VM is running and Docker responds:

```sh
kat status
docker --context katamaran info
```

## Uninstall

To remove the LaunchAgent, the Docker context, and the `~/.local/bin/kat` symlink, run:

```sh
kat uninstall
```

`kat uninstall` stops the daemon and leaves `~/.katamaran` intact so your containers, images, volumes, and Linux machines stay on disk. Delete `~/.katamaran` manually if you want to remove all stored data.
