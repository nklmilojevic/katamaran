import Foundation
import Metal
import QuartzCore
import CKatTerm

// The Metal grid renderer, adapted from Thurm's (Renderer.swift): instanced quads for
// backgrounds, glyphs from the atlases, decorations and the cursor.

// MARK: - Constants of kat_term.h

enum CellFlag {
    static let bold: UInt16 = 1 << 0
    static let italic: UInt16 = 1 << 1
    static let underline: UInt16 = 1 << 2
    static let doubleUnderline: UInt16 = 1 << 3
    static let undercurl: UInt16 = 1 << 4
    static let dottedUnderline: UInt16 = 1 << 5
    static let dashedUnderline: UInt16 = 1 << 6
    static let strikeout: UInt16 = 1 << 7
    static let wide: UInt16 = 1 << 8
    static let wideSpacer: UInt16 = 1 << 9
    static let hidden: UInt16 = 1 << 10
    static let selected: UInt16 = 1 << 11
    static let defaultBackground: UInt16 = 1 << 13
    static let dim: UInt16 = 1 << 14
}

/// `KAT_NO_COLOR`: underline color "use fg".
let kNoColor: UInt32 = 0xFF00_0000

enum TermMode {
    static let showCursor: UInt32 = 1 << 0
    static let mouseReportClick: UInt32 = 1 << 3
    static let bracketedPaste: UInt32 = 1 << 4
    static let mouseMotion: UInt32 = 1 << 6
    static let focusInOut: UInt32 = 1 << 11
    static let altScreen: UInt32 = 1 << 12
    static let mouseDrag: UInt32 = 1 << 13
    static let mouseAny: UInt32 = (1 << 3) | (1 << 6) | (1 << 13)
}

enum CursorShapeCode {
    static let block: UInt8 = 0
    static let underline: UInt8 = 1
    static let beam: UInt8 = 2
    static let hollowBlock: UInt8 = 3
    static let hidden: UInt8 = 4
}

// MARK: - GPU instance format

/// One instanced quad. Must match `struct Quad` in the shader (64 bytes).
struct QuadInstance {
    /// x, y, width, height in pixels (origin top-left).
    var rect: SIMD4<Float>
    /// Texture rectangle in texels (u0, v0, u1, v1), or kind specific parameters.
    var uv: SIMD4<Float>
    /// Straight (non premultiplied) RGBA.
    var color: SIMD4<Float>
    var kind: UInt32
    var pad0: UInt32 = 0
    var pad1: UInt32 = 0
    var pad2: UInt32 = 0

    init(rect: SIMD4<Float>, uv: SIMD4<Float> = SIMD4<Float>(0, 0, 0, 0), color: SIMD4<Float>, kind: UInt32) {
        self.rect = rect
        self.uv = uv
        self.color = color
        self.kind = kind
    }
}

enum QuadKind {
    static let solid: UInt32 = 0
    static let grayGlyph: UInt32 = 1
    static let colorGlyph: UInt32 = 2
    static let undercurl: UInt32 = 3
    static let dotted: UInt32 = 5
    static let dashed: UInt32 = 6
    static let arc: UInt32 = 7
}

/// Converts 0xRRGGBB to a float color.
@inline(__always)
func rgba(_ rgb: UInt32, _ alpha: Float = 1) -> SIMD4<Float> {
    SIMD4<Float>(Float((rgb >> 16) & 0xFF) / 255, Float((rgb >> 8) & 0xFF) / 255, Float(rgb & 0xFF) / 255, alpha)
}

// MARK: - Shader (compiled at runtime; SwiftPM cannot build .metal files)

private let shaderSource = """
#include <metal_stdlib>
using namespace metal;

struct Quad {
    float4 rect;
    float4 uv;
    float4 color;
    uint kind;
    uint pad0;
    uint pad1;
    uint pad2;
};

struct Uniforms {
    float2 viewport;
};

struct VOut {
    float4 position [[position]];
    float2 uv;
    float2 local;
    float2 size;
    float4 color;
    float4 params;
    uint kind [[flat]];
};

vertex VOut vs_main(uint vid [[vertex_id]],
                    uint iid [[instance_id]],
                    const device Quad *quads [[buffer(0)]],
                    constant Uniforms &u [[buffer(1)]]) {
    Quad q = quads[iid];
    float2 corner = float2(float(vid & 1u), float(vid >> 1u));
    float2 pos = q.rect.xy + corner * q.rect.zw;
    VOut o;
    o.position = float4(pos.x / u.viewport.x * 2.0 - 1.0, 1.0 - pos.y / u.viewport.y * 2.0, 0.0, 1.0);
    o.uv = mix(q.uv.xy, q.uv.zw, corner);
    o.local = corner * q.rect.zw;
    o.size = q.rect.zw;
    o.color = q.color;
    o.params = q.uv;
    o.kind = q.kind;
    return o;
}

fragment float4 fs_main(VOut in [[stage_in]],
                        texture2d<float> grayAtlas [[texture(0)]],
                        texture2d<float> colorAtlas [[texture(1)]]) {
    constexpr sampler nearestPx(coord::pixel, filter::nearest, address::clamp_to_edge);
    float4 c = in.color;
    switch (in.kind) {
    case 0u: {
        return float4(c.rgb * c.a, c.a);
    }
    case 1u: {
        float a = grayAtlas.sample(nearestPx, in.uv).r * c.a;
        return float4(c.rgb * a, a);
    }
    case 2u: {
        return colorAtlas.sample(nearestPx, in.uv) * c.a;
    }
    case 3u: {
        // Undercurl: params.x = thickness, params.y = wavelength, params.z = phase (quad x).
        float t = max(in.params.x, 1.0);
        float period = max(in.params.y, 2.0);
        float amp = max((in.size.y - t) * 0.5, 0.0);
        float k = 6.2831853 / period;
        float x = in.local.x + in.params.z;
        float y = in.size.y * 0.5 + amp * sin(x * k);
        float slope = amp * k * cos(x * k);
        float d = abs(in.local.y - y) / sqrt(1.0 + slope * slope);
        float a = clamp(t * 0.5 - d + 0.5, 0.0, 1.0) * c.a;
        return float4(c.rgb * a, a);
    }
    case 5u: {
        // Dotted: params.x = dot size, params.z = phase.
        float t = max(in.params.x, 1.0);
        float x = in.local.x + in.params.z;
        float a = (fmod(x, 2.0 * t) < t) ? c.a : 0.0;
        return float4(c.rgb * a, a);
    }
    case 6u: {
        // Dashed: params.y = dash length, params.z = phase.
        float dash = max(in.params.y, 2.0);
        float x = in.local.x + in.params.z;
        float a = (fmod(x, 2.0 * dash) < dash) ? c.a : 0.0;
        return float4(c.rgb * a, a);
    }
    case 7u: {
        // Quarter-circle stroke: params = (radius, thickness, center x, center y) in the quad.
        float d = abs(length(in.local - in.params.zw) - in.params.x);
        float a = clamp(in.params.y * 0.5 - d + 0.5, 0.0, 1.0) * c.a;
        return float4(c.rgb * a, a);
    }
    default:
        return float4(0.0);
    }
}
"""

// MARK: - Shared Metal state

final class MetalContext {
    static let shared: MetalContext? = MetalContext()

    let device: MTLDevice
    let queue: MTLCommandQueue
    let pipeline: MTLRenderPipelineState
    /// The most recently committed frame (all terminals share this queue).
    var lastCommitted: MTLCommandBuffer?

    /// Blocks until the GPU finished every committed frame, before the CPU rewrites a texture
    /// region those frames may still sample (a wiped glyph atlas that got no new texture).
    func waitForGPU() {
        if let cb = lastCommitted, cb.status != .completed, cb.status != .error {
            cb.waitUntilCompleted()
        }
    }

    private init?() {
        guard let device = MTLCreateSystemDefaultDevice(), let queue = device.makeCommandQueue() else {
            return nil
        }
        self.device = device
        self.queue = queue
        do {
            let library = try device.makeLibrary(source: shaderSource, options: nil)
            guard let vfn = library.makeFunction(name: "vs_main"),
                  let ffn = library.makeFunction(name: "fs_main")
            else { return nil }
            let desc = MTLRenderPipelineDescriptor()
            desc.label = "Terminal quads"
            desc.vertexFunction = vfn
            desc.fragmentFunction = ffn
            let att: MTLRenderPipelineColorAttachmentDescriptor = desc.colorAttachments[0]
            att.pixelFormat = .bgra8Unorm
            att.isBlendingEnabled = true
            att.rgbBlendOperation = .add
            att.alphaBlendOperation = .add
            att.sourceRGBBlendFactor = .one
            att.sourceAlphaBlendFactor = .one
            att.destinationRGBBlendFactor = .oneMinusSourceAlpha
            att.destinationAlphaBlendFactor = .oneMinusSourceAlpha
            pipeline = try device.makeRenderPipelineState(descriptor: desc)
        } catch {
            NSLog("terminal: could not build the Metal pipeline: \(error)")
            return nil
        }
    }
}

// MARK: - Grid snapshot

/// A copy of a terminal's grid, refreshed from `kat_term_snapshot`.
final class GridSnapshot {
    private(set) var info = kat_grid_info()
    private(set) var cells: [kat_cell] = []
    /// Grapheme clusters per row: column → full cluster text.
    private(set) var rowClusters: [[Int: String]] = []
    private(set) var valid = false

    var cols: Int { Int(info.cols) }
    var rows: Int { Int(info.rows) }

    /// Pulls the latest grid. Returns true when anything changed.
    func update(_ term: OpaquePointer) -> Bool {
        var newInfo = info
        if !valid { newInfo.generation = UInt64.max }
        var changed = cells.withUnsafeMutableBufferPointer {
            kat_term_snapshot(term, &newInfo, $0.baseAddress, $0.count)
        }
        guard changed else { return false }
        var count = Int(newInfo.cols) * Int(newInfo.rows)
        var attempts = 0
        while cells.count < count && attempts < 4 {
            attempts += 1
            // Too small for the new size: only `info` was filled.
            cells = [kat_cell](repeating: kat_cell(), count: count)
            newInfo.generation = UInt64.max
            changed = cells.withUnsafeMutableBufferPointer {
                kat_term_snapshot(term, &newInfo, $0.baseAddress, $0.count)
            }
            count = Int(newInfo.cols) * Int(newInfo.rows)
        }
        guard cells.count >= count else { return false }
        if cells.count > count {
            cells.removeLast(cells.count - count)
        }
        rowClusters = [[Int: String]](repeating: [:], count: Int(newInfo.rows))
        let total = kat_term_clusters(term, nil, 0)
        if total > 0 {
            var list = [kat_cluster](repeating: kat_cluster(), count: total)
            let n = list.withUnsafeMutableBufferPointer { kat_term_clusters(term, $0.baseAddress, total) }
            for var item in list.prefix(min(n, total)) {
                let r = Int(item.row)
                guard r < rowClusters.count else { continue }
                let len = Int(item.len)
                let text = withUnsafeBytes(of: &item.text) { raw in
                    String(decoding: raw.prefix(len), as: UTF8.self)
                }
                rowClusters[r][Int(item.col)] = text
            }
        }
        info = newInfo
        valid = true
        return true
    }

    func cell(col: Int, row: Int) -> kat_cell? {
        let idx = row * cols + col
        guard valid, col >= 0, row >= 0, col < cols, row < rows, idx < cells.count else { return nil }
        return cells[idx]
    }
}

// MARK: - Renderer

/// Everything besides the grid that affects a frame.
struct RenderParams {
    var shaper: FontShaper
    var padX: Int
    var padY: Int
    var viewportWidth: Int
    var viewportHeight: Int
    /// View is first responder of the key window (solid cursor); hollow otherwise.
    var focused: Bool
    /// Blink phase.
    var cursorOn: Bool
    var themeBackground: UInt32
}

private struct DrawSegment {
    var start: Int
    var count: Int
}

final class TerminalRenderer {
    let snapshot = GridSnapshot()

    private var backgrounds: [QuadInstance] = []
    private var cursorUnder: [QuadInstance] = []
    private var glyphQuads: [QuadInstance] = []
    private var decorations: [QuadInstance] = []
    private var cursorOver: [QuadInstance] = []
    /// Quad buffers reused round robin; `inFlight` keeps the CPU from rewriting one the GPU
    /// may still read.
    private static let bufferCount = 3
    private var quadBuffers: [MTLBuffer?] = Array(repeating: nil, count: bufferCount)
    private var quadBufferIndex = 0
    private let inFlight = DispatchSemaphore(value: bufferCount)
    private var allQuads: [QuadInstance] = []
    private var segments: [DrawSegment] = []

    /// Draws a frame; `presented` gets the time it reached the screen.
    @discardableResult
    func draw(layer: CAMetalLayer, params p: RenderParams, presented: ((CFTimeInterval) -> Void)? = nil) -> Bool {
        guard let ctx = MetalContext.shared else { return false }
        guard p.viewportWidth > 0, p.viewportHeight > 0 else { return false }
        // Rasterizing a glyph can wipe a full atlas; quads built before that point to stale
        // texels, so build again against the fresh atlas.
        let epochs = (p.shaper.glyphs.gray.epoch, p.shaper.glyphs.color.epoch)
        buildQuads(p)
        if epochs != (p.shaper.glyphs.gray.epoch, p.shaper.glyphs.color.epoch) {
            buildQuads(p)
        }

        guard let drawable = layer.nextDrawable() else { return false }
        let clear = rgba(snapshot.valid ? snapshot.info.background : p.themeBackground)
        let pass = MTLRenderPassDescriptor()
        pass.colorAttachments[0].texture = drawable.texture
        pass.colorAttachments[0].loadAction = .clear
        pass.colorAttachments[0].storeAction = .store
        pass.colorAttachments[0].clearColor = MTLClearColor(red: Double(clear.x), green: Double(clear.y),
                                                            blue: Double(clear.z), alpha: 1)
        guard let commands = ctx.queue.makeCommandBuffer(),
              let encoder = commands.makeRenderCommandEncoder(descriptor: pass)
        else { return false }

        // All quads in one buffer, in paint order.
        allQuads.removeAll(keepingCapacity: true)
        segments.removeAll(keepingCapacity: true)
        for quads in [backgrounds, cursorUnder, glyphQuads, decorations, cursorOver] where !quads.isEmpty {
            segments.append(DrawSegment(start: allQuads.count, count: quads.count))
            allQuads.append(contentsOf: quads)
        }

        encoder.setRenderPipelineState(ctx.pipeline)
        var uniforms = SIMD2<Float>(Float(p.viewportWidth), Float(p.viewportHeight))
        encoder.setVertexBytes(&uniforms, length: MemoryLayout<SIMD2<Float>>.stride, index: 1)
        encoder.setFragmentTexture(p.shaper.glyphs.gray.texture, index: 0)
        encoder.setFragmentTexture(p.shaper.glyphs.color.texture, index: 1)

        if !allQuads.isEmpty {
            let stride = MemoryLayout<QuadInstance>.stride
            if let buffer = nextQuadBuffer(device: ctx.device, length: allQuads.count * stride) {
                allQuads.withUnsafeBytes { raw in
                    if let base = raw.baseAddress { buffer.contents().copyMemory(from: base, byteCount: raw.count) }
                }
                let inFlight = self.inFlight
                commands.addCompletedHandler { _ in inFlight.signal() }
                encoder.setVertexBuffer(buffer, offset: 0, index: 0)
                for seg in segments {
                    encoder.setVertexBufferOffset(seg.start * stride, index: 0)
                    encoder.drawPrimitives(type: .triangleStrip, vertexStart: 0, vertexCount: 4,
                                           instanceCount: seg.count)
                }
            }
        }
        encoder.endEncoding()
        if let presented {
            // presentedTime is 0 when the system has no timestamp for it: then when it was handed over.
            drawable.addPresentedHandler { d in presented(d.presentedTime > 0 ? d.presentedTime : CACurrentMediaTime()) }
        }
        if layer.presentsWithTransaction {
            commands.commit()
            commands.waitUntilScheduled()
            drawable.present()
        } else {
            commands.present(drawable)
            commands.commit()
        }
        ctx.lastCommitted = commands
        return true
    }

    /// The next quad buffer, at least `length` bytes. Waits while the GPU still reads every
    /// buffer; on success the caller signals `inFlight` when its command buffer completes.
    private func nextQuadBuffer(device: MTLDevice, length: Int) -> MTLBuffer? {
        inFlight.wait()
        quadBufferIndex = (quadBufferIndex + 1) % Self.bufferCount
        if let buffer = quadBuffers[quadBufferIndex], buffer.length >= length { return buffer }
        let size = max(64 * 1024, length + length / 2)
        guard let buffer = device.makeBuffer(length: size, options: [.storageModeShared]) else {
            inFlight.signal()
            return nil
        }
        quadBuffers[quadBufferIndex] = buffer
        return buffer
    }

    // MARK: Quad building

    private func buildQuads(_ p: RenderParams) {
        backgrounds.removeAll(keepingCapacity: true)
        cursorUnder.removeAll(keepingCapacity: true)
        glyphQuads.removeAll(keepingCapacity: true)
        decorations.removeAll(keepingCapacity: true)
        cursorOver.removeAll(keepingCapacity: true)
        if snapshot.valid { buildGrid(p) }
    }

    private func backgroundColor(_ cell: kat_cell, _ info: kat_grid_info) -> UInt32? {
        let f = cell.flags
        if f & CellFlag.selected != 0 { return info.selection_bg }
        if f & CellFlag.defaultBackground != 0 { return nil }
        return cell.bg
    }

    private func foregroundColor(_ cell: kat_cell, _ info: kat_grid_info) -> UInt32 {
        if cell.flags & CellFlag.selected != 0 { return info.selection_fg }
        return cell.fg
    }

    private func buildGrid(_ p: RenderParams) {
        let info = snapshot.info
        let cols = snapshot.cols
        let rows = snapshot.rows
        guard cols > 0, rows > 0, snapshot.cells.count >= cols * rows else { return }
        let shaper = p.shaper
        let cw = shaper.cellWidth
        let ch = shaper.cellHeight
        let fcw = Float(cw)
        let fch = Float(ch)
        let padX = Float(p.padX)
        let padY = Float(p.padY)

        // Cursor geometry.
        let cursorRow = Int(info.cursor_row)
        let cursorCol = Int(info.cursor_col)
        let cursorShown = (info.modes & TermMode.showCursor) != 0
            && info.cursor_shape != CursorShapeCode.hidden
            && info.display_offset == 0
            && cursorRow < rows && cursorCol < cols
            && (p.cursorOn || !p.focused)
        var shape = info.cursor_shape
        if !p.focused { shape = CursorShapeCode.hollowBlock }
        let blockCursor = cursorShown && shape == CursorShapeCode.block

        snapshot.cells.withUnsafeBufferPointer { allCells in
            for r in 0..<rows {
                let rowCells = UnsafeBufferPointer(rebasing: allCells[(r * cols)..<((r + 1) * cols)])
                let y = padY + Float(r) * fch

                // Backgrounds, merging horizontal runs of the same color.
                var runStart = -1
                var runColor: UInt32 = 0
                for c in 0...cols {
                    let color: UInt32? = c < cols ? backgroundColor(rowCells[c], info) : nil
                    if let color = color, runStart >= 0, color == runColor { continue }
                    if runStart >= 0 {
                        backgrounds.append(QuadInstance(
                            rect: SIMD4<Float>(padX + Float(runStart) * fcw, y, Float(c - runStart) * fcw, fch),
                            color: rgba(runColor), kind: QuadKind.solid))
                        runStart = -1
                    }
                    if let color = color {
                        runStart = c
                        runColor = color
                    }
                }

                // Text.
                let clusters = r < snapshot.rowClusters.count ? snapshot.rowClusters[r] : [:]
                let shaped = shaper.shapeRow(rowCells, clusters: clusters)
                let baselineY = y + Float(shaper.baseline)
                for g in shaped {
                    guard g.col < cols else { continue }
                    let cell = rowCells[g.col]
                    if cell.flags & CellFlag.hidden != 0 { continue }
                    var fg = foregroundColor(cell, info)
                    if blockCursor && r == cursorRow && g.col == cursorCol {
                        fg = info.cursor_text_color
                    }
                    let entry = shaper.glyphs.entry(for: GlyphKey(font: g.font, glyph: g.glyph),
                                                    font: shaper.font(g.font), isColor: g.color,
                                                    cell: CellBox(width: cw, height: ch, baseline: shaper.baseline))
                    if entry.isEmpty { continue }
                    let gx = (padX + Float(g.col) * fcw + g.x.rounded() + entry.left).rounded()
                    let gy = (baselineY - g.y.rounded() - entry.top).rounded()
                    glyphQuads.append(QuadInstance(
                        rect: SIMD4<Float>(gx, gy, entry.w, entry.h),
                        uv: SIMD4<Float>(entry.x, entry.y, entry.x + entry.w, entry.y + entry.h),
                        color: entry.color ? SIMD4<Float>(1, 1, 1, 1) : rgba(fg),
                        kind: entry.color ? QuadKind.colorGlyph : QuadKind.grayGlyph))
                }

                // Procedural box drawing / block elements, and decorations.
                let thickness = Float(shaper.lineThickness)
                for c in 0..<cols {
                    let cell = rowCells[c]
                    let f = cell.flags
                    if f & CellFlag.wideSpacer != 0 { continue }
                    let x = padX + Float(c) * fcw
                    var fg = foregroundColor(cell, info)
                    if blockCursor && r == cursorRow && c == cursorCol {
                        fg = info.cursor_text_color
                    }
                    if f & CellFlag.hidden == 0 && BoxDrawing.isProcedural(cell.ch) && clusters[c] == nil {
                        for b in shaper.boxRects(for: cell.ch) {
                            glyphQuads.append(QuadInstance(
                                rect: SIMD4<Float>(x + Float(b.x), y + Float(b.y), Float(b.w), Float(b.h)),
                                uv: b.arc ?? SIMD4<Float>(0, 0, 0, 0),
                                color: rgba(fg, b.alpha), kind: b.arc == nil ? QuadKind.solid : QuadKind.arc))
                        }
                    }

                    let width = (f & CellFlag.wide) != 0 ? fcw * 2 : fcw
                    var style = 0
                    if f & CellFlag.undercurl != 0 { style = 3 }
                    else if f & CellFlag.doubleUnderline != 0 { style = 2 }
                    else if f & CellFlag.dottedUnderline != 0 { style = 4 }
                    else if f & CellFlag.dashedUnderline != 0 { style = 5 }
                    else if f & CellFlag.underline != 0 { style = 1 }
                    if style != 0 {
                        let ulColor = (cell.ul & kNoColor) != 0 ? fg : cell.ul
                        let color = rgba(ulColor)
                        let uy = min(baselineY + Float(shaper.underlineOffset), y + fch - thickness)
                        switch style {
                        case 1:
                            decorations.append(QuadInstance(rect: SIMD4<Float>(x, uy, width, thickness),
                                                            color: color, kind: QuadKind.solid))
                        case 2:
                            let second = min(uy + thickness * 2, y + fch - thickness)
                            let first = max(y, second - thickness * 2)
                            decorations.append(QuadInstance(rect: SIMD4<Float>(x, first, width, thickness),
                                                            color: color, kind: QuadKind.solid))
                            decorations.append(QuadInstance(rect: SIMD4<Float>(x, second, width, thickness),
                                                            color: color, kind: QuadKind.solid))
                        case 3:
                            let h = max(thickness * 3, min(fch * 0.2, thickness * 5))
                            let top = min(uy - h / 2 + thickness / 2, y + fch - h)
                            decorations.append(QuadInstance(rect: SIMD4<Float>(x, top, width, h),
                                                            uv: SIMD4<Float>(thickness, fcw, x, 0),
                                                            color: color, kind: QuadKind.undercurl))
                        case 4:
                            decorations.append(QuadInstance(rect: SIMD4<Float>(x, uy, width, thickness),
                                                            uv: SIMD4<Float>(thickness, 0, x, 0),
                                                            color: color, kind: QuadKind.dotted))
                        default:
                            decorations.append(QuadInstance(rect: SIMD4<Float>(x, uy, width, thickness),
                                                            uv: SIMD4<Float>(thickness, max(2, fcw * 0.4), x, 0),
                                                            color: color, kind: QuadKind.dashed))
                        }
                    }
                    if f & CellFlag.strikeout != 0 {
                        let sy = baselineY - Float(shaper.strikeOffset) - thickness / 2
                        decorations.append(QuadInstance(rect: SIMD4<Float>(x, sy.rounded(), width, thickness),
                                                        color: rgba(fg), kind: QuadKind.solid))
                    }
                }
            }
        }

        // Cursor.
        if cursorShown {
            let color = rgba(info.cursor_color)
            let x = padX + Float(cursorCol) * fcw
            let y = padY + Float(cursorRow) * fch
            let w = info.cursor_wide ? fcw * 2 : fcw
            let t = Float(max(1, Int((2 * shaper.scale).rounded())))
            switch shape {
            case CursorShapeCode.block:
                cursorUnder.append(QuadInstance(rect: SIMD4<Float>(x, y, w, fch), color: color, kind: QuadKind.solid))
            case CursorShapeCode.beam:
                cursorOver.append(QuadInstance(rect: SIMD4<Float>(x, y, t, fch), color: color, kind: QuadKind.solid))
            case CursorShapeCode.underline:
                cursorOver.append(QuadInstance(rect: SIMD4<Float>(x, y + fch - t, w, t), color: color,
                                               kind: QuadKind.solid))
            default:
                let b = max(1, (t / 2).rounded())
                cursorOver.append(QuadInstance(rect: SIMD4<Float>(x, y, w, b), color: color, kind: QuadKind.solid))
                cursorOver.append(QuadInstance(rect: SIMD4<Float>(x, y + fch - b, w, b), color: color,
                                               kind: QuadKind.solid))
                cursorOver.append(QuadInstance(rect: SIMD4<Float>(x, y, b, fch), color: color, kind: QuadKind.solid))
                cursorOver.append(QuadInstance(rect: SIMD4<Float>(x + w - b, y, b, fch), color: color,
                                               kind: QuadKind.solid))
            }
        }
    }
}
