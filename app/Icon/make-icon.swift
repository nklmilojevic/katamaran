// Draws the Katamaran app icon with Core Graphics and writes an .iconset.
// Usage: swift make-icon.swift <out.iconset>

import CoreGraphics
import Foundation
import ImageIO
import UniformTypeIdentifiers

let canvas: CGFloat = 1024

func rgb(_ hex: UInt32, _ alpha: CGFloat = 1) -> CGColor {
    CGColor(
        srgbRed: CGFloat((hex >> 16) & 0xff) / 255,
        green: CGFloat((hex >> 8) & 0xff) / 255,
        blue: CGFloat(hex & 0xff) / 255,
        alpha: alpha)
}

/// Apple's icon grid: an 824 pt superellipse centred on a 1024 pt canvas.
func squircle(in rect: CGRect) -> CGPath {
    let p = CGMutablePath()
    let (cx, cy, a) = (rect.midX, rect.midY, rect.width / 2)
    let n: CGFloat = 5
    for i in 0...720 {
        let t = CGFloat(i) / 720 * 2 * .pi
        let (c, s) = (cos(t), sin(t))
        let x = cx + a * (c < 0 ? -1 : 1) * pow(abs(c), 2 / n)
        let y = cy + a * (s < 0 ? -1 : 1) * pow(abs(s), 2 / n)
        if i == 0 { p.move(to: CGPoint(x: x, y: y)) } else { p.addLine(to: CGPoint(x: x, y: y)) }
    }
    p.closeSubpath()
    return p
}

func draw(_ ctx: CGContext) {
    let tile = CGRect(x: 100, y: 100, width: 824, height: 824)
    let shape = squircle(in: tile)
    let background = rgb(0x1c1f26)
    let boat = rgb(0xf4f6fa)
    let water = rgb(0x38bdf8)

    ctx.saveGState()
    ctx.setShadow(offset: CGSize(width: 0, height: -10), blur: 28, color: rgb(0x000000, 0.35))
    ctx.addPath(shape)
    ctx.setFillColor(background)
    ctx.fillPath()
    ctx.restoreGState()

    ctx.setFillColor(boat)

    // Mainsail: tall, aft of the mast, with a curved leech.
    let mastX: CGFloat = 500
    let main = CGMutablePath()
    main.move(to: CGPoint(x: mastX + 22, y: 790))
    main.addCurve(
        to: CGPoint(x: 705, y: 480), control1: CGPoint(x: 615, y: 690),
        control2: CGPoint(x: 695, y: 580))
    main.addLine(to: CGPoint(x: mastX + 22, y: 480))
    main.closeSubpath()
    ctx.addPath(main)
    ctx.fillPath()

    // Jib: smaller, forward of the mast.
    let jib = CGMutablePath()
    jib.move(to: CGPoint(x: mastX - 22, y: 725))
    jib.addLine(to: CGPoint(x: mastX - 22, y: 480))
    jib.addLine(to: CGPoint(x: 330, y: 480))
    jib.addCurve(
        to: CGPoint(x: mastX - 22, y: 725), control1: CGPoint(x: 390, y: 590),
        control2: CGPoint(x: 450, y: 675))
    jib.closeSubpath()
    ctx.addPath(jib)
    ctx.fillPath()

    // Mast.
    ctx.fill(CGRect(x: mastX - 9, y: 450, width: 18, height: 360))

    // Hull: flat deck, rounded bottom.
    let hull = CGMutablePath()
    hull.move(to: CGPoint(x: 270, y: 452))
    hull.addLine(to: CGPoint(x: 754, y: 452))
    hull.addCurve(
        to: CGPoint(x: 662, y: 385), control1: CGPoint(x: 740, y: 415),
        control2: CGPoint(x: 708, y: 385))
    hull.addLine(to: CGPoint(x: 362, y: 385))
    hull.addCurve(
        to: CGPoint(x: 270, y: 452), control1: CGPoint(x: 318, y: 385),
        control2: CGPoint(x: 284, y: 415))
    hull.closeSubpath()
    ctx.addPath(hull)
    ctx.fillPath()

    // Two waves.
    ctx.setStrokeColor(water)
    ctx.setLineWidth(26)
    ctx.setLineCap(.round)
    ctx.setLineJoin(.round)
    for y in [CGFloat(322), 250] {
        var x: CGFloat = 280
        ctx.move(to: CGPoint(x: x, y: y + 16 * sin((x / 155 + 0.25) * 2 * .pi)))
        while x <= 744 {
            x += 4
            ctx.addLine(to: CGPoint(x: x, y: y + 16 * sin((x / 155 + 0.25) * 2 * .pi)))
        }
        ctx.strokePath()
    }

    // Hairline border so the tile holds up on dark backgrounds.
    ctx.addPath(shape)
    ctx.setStrokeColor(rgb(0xffffff, 0.12))
    ctx.setLineWidth(2)
    ctx.strokePath()
}

func render(size: Int) -> CGImage {
    let ctx = CGContext(
        data: nil, width: size, height: size, bitsPerComponent: 8, bytesPerRow: 0,
        space: CGColorSpace(name: CGColorSpace.sRGB)!,
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue)!
    ctx.interpolationQuality = .high
    ctx.scaleBy(x: CGFloat(size) / canvas, y: CGFloat(size) / canvas)
    draw(ctx)
    return ctx.makeImage()!
}

func write(_ image: CGImage, to url: URL) {
    let dest = CGImageDestinationCreateWithURL(url as CFURL, UTType.png.identifier as CFString, 1, nil)!
    CGImageDestinationAddImage(dest, image, nil)
    precondition(CGImageDestinationFinalize(dest), "writing \(url.path)")
}

let out = URL(fileURLWithPath: CommandLine.arguments.dropFirst().first ?? "Katamaran.iconset")
try FileManager.default.createDirectory(at: out, withIntermediateDirectories: true)
for base in [16, 32, 128, 256, 512] {
    write(render(size: base), to: out.appendingPathComponent("icon_\(base)x\(base).png"))
    write(render(size: base * 2), to: out.appendingPathComponent("icon_\(base)x\(base)@2x.png"))
}
