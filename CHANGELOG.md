# Changelog

## v0.4.0 - 2026-11-20

This release makes it easier to get Katamaran. It also fixes the `binfmt_misc` mask in `kat-init`.

### New features

- The site `katamaran.rs` now has a **Download for macOS** button on the landing page. The `/download/` page and the install page link directly to the latest `Katamaran.dmg`.

### Fixes

- `kat-init` now matches `EI_DATA` and `EI_VERSION` in the `binfmt_misc` mask. This makes the ELF binary check more exact.

### Other changes

- The documentation site now loads from the root of `katamaran.rs`.
- The `toml` crate is now at version 1.2.1.

## v0.3.0 - 2026-11-20

This release adds a guide for coding agents. An agent can now learn how to use katamaran with the `kat skill` command.

### New features

- Add the `kat skill` command. Use `kat skill show` to print a guide for coding agents. The guide covers VM use, Docker, networking, and more.
- Add `kat skill setup`, `kat skill status`, and `kat skill remove` to manage the skill for Claude Code, Codex, OpenCode, and Pi. The installed skill is a small stub that calls `kat skill show`, so the guide stays current when you upgrade katamaran.

## v0.2.0 - 2026-11-20

This release makes `katamaran` the default Docker context when you start the app. It also updates the documentation site with screenshots and a new design.

### New features

- `Katamaran.app` now sets `katamaran` as the default Docker context on launch. The first launch runs `katamaran setup --default`, and later launches run `katamaran use-context`.
- `kat start` now makes sure the `katamaran` Docker context is active. You do not need to switch the context by hand.

### Fixes

- `katamaran` now finds `docker` in the standard macOS install locations. The Docker context steps now work when you open the app from Finder, the Dock, or Spotlight.

### Other changes

- The README and the documentation site now show screenshots of the macOS app. The install, quickstart, and Docker pages describe the automatic default Docker context.
- The documentation site has a new design. It uses the Allo and JetBrains Mono fonts, a midnight-indigo color palette, and Tokyo Night code highlighting.
- The documentation site moves to the `katamaran.rs` domain. The default Sparkle feed URL is now `https://katamaran.rs/appcast.xml`.

## v0.1.1 - 2026-11-20

This release explains how to install katamaran with Homebrew and removes build warnings from the libkrun code.

### Other changes

- Add instructions for the Homebrew cask install to the README and the docs. Users can now find the install steps in one place.
- Remove the deprecated `libc` `VM_MEMORY_APPLICATION_SPECIFIC_1` warnings in the libkrun `vstate` code. The build output is now clean.

## v0.1.0 - 2026-11-20

This is the first release of katamaran. It gives you the first version of the project and its documentation site.

### New features

- Add the first version of katamaran, a macOS app that runs virtual machines and containers.

### Other changes

- Set up the documentation site to work from the `/katamaran` path on GitHub Pages. The links and files on the site now load correctly.
