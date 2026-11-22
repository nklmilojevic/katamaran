import Foundation

/// Dedicated GCD queues for blocking Unix-socket and child-process I/O so it
/// never runs on Swift Concurrency's cooperative thread pool.
enum BlockingIO {
    static let daemon = DispatchQueue(label: "katamaran.io.daemon", qos: .userInitiated, attributes: .concurrent)
    /// Serial queue so overlapping `config.set` calls never race or revert each other.
    static let config = DispatchQueue(label: "katamaran.io.config", qos: .userInitiated)
    static let docker = DispatchQueue(label: "katamaran.io.docker", qos: .userInitiated, attributes: .concurrent)
    static let buildx = DispatchQueue(label: "katamaran.io.buildx", qos: .utility, attributes: .concurrent)
    static let logs = DispatchQueue(label: "katamaran.io.logs", qos: .utility, attributes: .concurrent)

    static func run<T>(
        on queue: DispatchQueue,
        _ work: @escaping () -> T,
        completion: @escaping @MainActor (T) -> Void
    ) {
        queue.async {
            let result = work()
            DispatchQueue.main.async {
                MainActor.assumeIsolated { completion(result) }
            }
        }
    }
}
