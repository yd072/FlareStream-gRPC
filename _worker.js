import { connect } from 'cloudflare:sockets';

let DEFAULT_UUID = 'd342d11e-d424-4583-b36e-524ab1f0afa4';

function hexToBytes(hex) {
    const bytes = new Uint8Array(16);
    for (let i = 0; i < 16; i++) {
        bytes[i] = parseInt(hex.substring(i * 2, i * 2 + 2), 16);
    }
    return bytes;
}

export default {
    async fetch(request, env, ctx) {
        const contentType = request.headers.get("Content-Type") || "";
        
        if (request.method !== "POST" || !contentType.startsWith("application/grpc")) {
            return new Response("Not Found", { status: 404 });
        }

        const uuidStr = env.UUID || DEFAULT_UUID;
        const EXPECTED_UUID = hexToBytes(uuidStr.replace(/-/g, ''));

        try {
            return await processGrpcStream(request, EXPECTED_UUID);
        } catch (err) {
            return new Response("Internal Error", { status: 500 });
        }
    }
};

async function processGrpcStream(request, EXPECTED_UUID) {
    let proxyIP = null;
    let proxyPort = 443;
    
    const url = new URL(request.url);
    
    let proxyIpParam = url.searchParams.get('proxyip') || url.searchParams.get('ip');

    if (!proxyIpParam) {
        const pathStr = url.pathname;
        const pIdx = pathStr.indexOf('proxyip=');
        if (pIdx !== -1) {
            proxyIpParam = pathStr.substring(pIdx + 8);
        } else {
            const iIdx = pathStr.indexOf('ip=');
            if (iIdx !== -1) {
                proxyIpParam = pathStr.substring(iIdx + 3);
            }
        }
    }
    
    if (proxyIpParam) {
        try { proxyIpParam = decodeURIComponent(proxyIpParam); } catch (e) {}
        const slashIndex = proxyIpParam.indexOf('/');
        if (slashIndex !== -1) proxyIpParam = proxyIpParam.substring(0, slashIndex);
        const andIndex = proxyIpParam.indexOf('&');
        if (andIndex !== -1) proxyIpParam = proxyIpParam.substring(0, andIndex);

        if (proxyIpParam.startsWith('[')) {
            const endBracket = proxyIpParam.indexOf(']');
            if (endBracket !== -1) {
                proxyIP = proxyIpParam.substring(1, endBracket);
                const portPart = proxyIpParam.substring(endBracket + 1);
                if (portPart.startsWith(':')) proxyPort = parseInt(portPart.substring(1), 10);
            }
        } else {
            const parts = proxyIpParam.split(':');
            if (parts.length === 2) {
                proxyIP = parts[0];
                proxyPort = parseInt(parts[1], 10);
            } else {
                proxyIP = proxyIpParam;
            }
        }
    }

    const unwrappedStream = request.body.pipeThrough(createGrpcUnwrapper());
    const reader = unwrappedStream.getReader();
    
    let buffer = new Uint8Array(0);
    let parsed = null;
    
    while (true) {
        const { value, done } = await reader.read();
        if (done && buffer.byteLength === 0) return new Response("Bad Request", { status: 400 });
        if (value) {
            let temp = new Uint8Array(buffer.byteLength + value.byteLength);
            temp.set(buffer);
            temp.set(value, buffer.byteLength);
            buffer = temp;
        }
        
        if (buffer.byteLength >= 24) {
            if (buffer[0] !== 0) return new Response("Bad Version", { status: 400 });

            let uuidMatch = true;
            for (let i = 0; i < 16; i++) {
                if (buffer[i + 1] !== EXPECTED_UUID[i]) {
                    uuidMatch = false;
                    break;
                }
            }
            if (!uuidMatch) return new Response("Not Found", { status: 404 });

            const addonLen = buffer[17];
            let offset = 18 + addonLen;
            if (offset < buffer.byteLength) {
                const command = buffer[offset++];
                if (offset + 1 < buffer.byteLength) {
                    const port = (buffer[offset] << 8) | buffer[offset + 1];
                    offset += 2;
                    if (offset < buffer.byteLength) {
                        const addrType = buffer[offset++];
                        let address = "";
                        let valid = false;
                        
                        if (addrType === 1 && offset + 3 < buffer.byteLength) { 
                            address = buffer.subarray(offset, offset + 4).join('.');
                            offset += 4;
                            valid = true;
                        } else if (addrType === 2 && offset < buffer.byteLength) { 
                            const len = buffer[offset++];
                            if (offset + len <= buffer.byteLength) {
                                address = new TextDecoder().decode(buffer.subarray(offset, offset + len));
                                offset += len;
                                valid = true;
                            }
                        } else if (addrType === 3 && offset + 15 < buffer.byteLength) { 
                            const parts = [];
                            for (let i = 0; i < 8; i++) {
                                parts.push((buffer[offset + i * 2] << 8 | buffer[offset + i * 2 + 1]).toString(16));
                            }
                            address = parts.join(':');
                            offset += 16;
                            valid = true;
                        }
                        
                        if (valid) {
                            const rawData = buffer.subarray(offset);
                            parsed = { command, port, address, rawData };
                            break;
                        }
                    }
                }
            }
        }
        if (done) break;
    }
    
    if (!parsed) return new Response("Bad Request", { status: 400 });
    const { command, port, address, rawData } = parsed;
    if (command === 2) return new Response("UDP Not Supported", { status: 403 });
    
    let socket;
    try {
        socket = connect({ hostname: address, port: port });
        await socket.opened; 
    } catch (err) {
        if (proxyIP) {
            try {
                socket = connect({ hostname: proxyIP, port: proxyPort });
                await socket.opened;
            } catch (fErr) { return new Response("Proxy IP Failed", { status: 502 }); }
        } else { return new Response("Connect Failed", { status: 502 }); }
    }
    
    try {
        const writer = socket.writable.getWriter();
        if (rawData.byteLength > 0) await writer.write(rawData);
        writer.releaseLock();
        
        const restOfRequest = new ReadableStream({
            async pull(controller) {
                try {
                    const { value, done } = await reader.read();
                    if (done) controller.close(); else controller.enqueue(value);
                } catch (e) { controller.error(e); }
            },
            cancel() { reader.cancel(); }
        });
        restOfRequest.pipeTo(socket.writable).catch(() => {});
        
        const responseStream = socket.readable.pipeThrough(createGrpcWrapper());
        return new Response(responseStream, {
            status: 200,
            headers: { "Content-Type": "application/grpc" }
        });
    } catch (e) { return new Response("Stream Error", { status: 502 }); }
}

function createGrpcUnwrapper() {
    let leftover = null;
    return new TransformStream({
        transform(chunk, controller) {
            let buffer = leftover ? (() => {
                let t = new Uint8Array(leftover.byteLength + chunk.byteLength);
                t.set(leftover); t.set(chunk, leftover.byteLength);
                return t;
            })() : chunk;
            let offset = 0;
            while (offset + 5 <= buffer.byteLength) {
                let length = ((buffer[offset + 1] << 24) | (buffer[offset + 2] << 16) | (buffer[offset + 3] << 8) | buffer[offset + 4]) >>> 0;
                if (offset + 5 + length <= buffer.byteLength) {
                    let frame = buffer.subarray(offset + 5, offset + 5 + length);
                    if (frame.byteLength > 0 && frame[0] === 0x0A) {
                        let p = 1;
                        while (p < frame.byteLength && (frame[p] & 0x80) !== 0) p++;
                        p++;
                        if (p < frame.byteLength) controller.enqueue(frame.subarray(p));
                    }
                    offset += 5 + length;
                } else break;
            }
            leftover = offset < buffer.byteLength ? buffer.subarray(offset) : null;
        }
    });
}

function createGrpcWrapper() {
    return new TransformStream({
        start(controller) { controller.enqueue(createGrpcFrame(new Uint8Array([0, 0]))); },
        transform(chunk, controller) { controller.enqueue(createGrpcFrame(chunk)); }
    });
}

function createGrpcFrame(rawData) {
    let L = rawData.byteLength;
    let varintLen = 1, n = L;
    while (n >= 0x80) { varintLen++; n >>>= 7; }
    let M = 1 + varintLen + L; 
    let frame = new Uint8Array(5 + M); 
    frame[0] = 0; 
    frame[1] = (M >>> 24) & 0xff; frame[2] = (M >>> 16) & 0xff; frame[3] = (M >>> 8) & 0xff; frame[4] = M & 0xff;
    frame[5] = 0x0A;
    let offset = 6; n = L;
    while (n >= 0x80) { frame[offset++] = (n & 0x7f) | 0x80; n >>>= 7; }
    frame[offset++] = n;
    frame.set(rawData, offset);
    return frame;
}
