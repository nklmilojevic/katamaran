// swift-tools-version:6.0
import PackageDescription

// The terminal engine (crates/kat-term) is a Rust static library: `cargo xtask app` builds it
// into this directory first. KAT_TERM_LIB_DIR overrides it.
let katTermLib = Context.environment["KAT_TERM_LIB_DIR"]
    ?? (Context.packageDirectory + "/../target/aarch64-apple-darwin/release")

let package = Package(
    name: "KatamaranApp",
    platforms: [.macOS(.v15)],
    targets: [
        .systemLibrary(name: "CKatTerm", path: "Sources/CKatTerm"),
        // Automatic updates. `cargo xtask app` embeds the framework in Contents/Frameworks.
        // The archive also carries Sparkle's CLI tools (`generate_keys`, `sign_update`).
        .binaryTarget(
            name: "Sparkle",
            url: "https://github.com/sparkle-project/Sparkle/releases/download/2.10.0/Sparkle-for-Swift-Package-Manager.zip",
            checksum: "17e28312b8e18ab7cdbbe09a6fb28cc55a5479ec6c371dbc07cdecd2a14fd959"
        ),
        .executableTarget(
            name: "KatamaranApp",
            dependencies: ["CKatTerm", "Sparkle"],
            path: "Sources/KatamaranApp",
            swiftSettings: [.swiftLanguageMode(.v5)],
            linkerSettings: [
                .unsafeFlags(["-L", katTermLib, "-Xlinker", "-rpath", "-Xlinker", "@executable_path/../Frameworks"]),
                .linkedFramework("Metal"),
                .linkedFramework("QuartzCore"),
                .linkedFramework("CoreText"),
            ]
        ),
        .testTarget(
            name: "KatamaranAppTests",
            dependencies: ["KatamaranApp"],
            path: "Tests/KatamaranAppTests",
            resources: [.copy("Fixtures")],
            swiftSettings: [.swiftLanguageMode(.v5)]
        ),
    ]
)
