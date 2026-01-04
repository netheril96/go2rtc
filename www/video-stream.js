import { VideoRTC } from './video-rtc.js';

/**
 * This is example, how you can extend VideoRTC player for your app.
 * Also you can check this example: https://github.com/AlexxIT/WebRTC
 */
class VideoStream extends VideoRTC {
    set divMode(value) {
        this.querySelector('.mode').innerText = value;
        this.querySelector('.status').innerText = '';
    }

    set divError(value) {
        const state = this.querySelector('.mode').innerText;
        if (state !== 'loading') return;
        this.querySelector('.mode').innerText = 'error';
        this.querySelector('.status').innerText = value;
    }

    ptz(p, t, z) {
        const url = new URL(this.wsURL);
        if (url.protocol === 'ws:') url.protocol = 'http:';
        if (url.protocol === 'wss:') url.protocol = 'https:';
        url.pathname = 'api/ptz';

        const src = url.searchParams.get('src');
        url.search = '';

        fetch(url, {
            method: 'POST',
            body: JSON.stringify({ name: src, p: p, t: t, z: z })
        });
    }

    /**
     * Custom GUI
     */
    oninit() {
        let iceServers = localStorage.getItem('iceServers');
        if (!iceServers) {
            const defaults = '[{"urls":["stun:"]}]';
            iceServers = prompt('ICE servers', defaults);
            if (iceServers) {
                localStorage.setItem('iceServers', iceServers);
            } else {
                iceServers = defaults;
            }
        }
        this.pcConfig.iceServers = JSON.parse(iceServers);

        if (new URLSearchParams(location.search).get('microphone') === 'true') {
            this.media = 'video,audio,microphone';
        }

        console.debug('stream.oninit');
        super.oninit();

        this.innerHTML = `
        <style>
        video-stream {
            position: relative;
        }
        .info {
            position: absolute;
            top: 0;
            left: 0;
            right: 0;
            padding: 12px;
            color: white;
            display: flex;
            justify-content: space-between;
            pointer-events: none;
        }
        .ptz {
            position: absolute;
            bottom: 10px;
            right: 10px;
            display: grid;
            grid-template-columns: repeat(3, 30px);
            gap: 5px;
            opacity: 0;
            transition: opacity 0.5s;
        }
        video-stream:hover .ptz {
            opacity: 1;
        }
        .ptz > button {
            height: 30px;
            cursor: pointer;
            background-color: rgba(255, 255, 255, 0.2);
            border: none;
            color: white;
            font-size: 16px;
        }
        .ptz > button:hover {
            background-color: rgba(255, 255, 255, 0.5);
            color: black;
        }
        </style>
        <div class="info">
            <div class="status"></div>
            <div class="mode"></div>
        </div>
        <div class="ptz">
            <button style="grid-column: 2" data-ptz="0,0.1,0">▲</button>
            <button style="grid-column: 1; grid-row: 2" data-ptz="-0.05,0,0">◀</button>
            <button style="grid-column: 2; grid-row: 2" data-ptz="0,-0.1,0">▼</button>
            <button style="grid-column: 3; grid-row: 2" data-ptz="0.05,0,0">▶</button>
        </div>
        `;

        const info = this.querySelector('.info');
        this.insertBefore(this.video, info);

        this.querySelector('.ptz').addEventListener('click', ev => {
            const btn = ev.target.closest('button');
            if (btn) {
                const args = btn.dataset.ptz.split(',').map(parseFloat);
                this.ptz(...args);
            }
        });
    }

    onconnect() {
        console.debug('stream.onconnect');
        const result = super.onconnect();
        if (result) this.divMode = 'loading';
        return result;
    }

    ondisconnect() {
        console.debug('stream.ondisconnect');
        super.ondisconnect();
    }

    onopen() {
        console.debug('stream.onopen');
        const result = super.onopen();

        this.onmessage['stream'] = msg => {
            console.debug('stream.onmessge', msg);
            switch (msg.type) {
                case 'error':
                    this.divError = msg.value;
                    break;
                case 'mse':
                case 'hls':
                case 'mp4':
                case 'mjpeg':
                    this.divMode = msg.type.toUpperCase();
                    break;
            }
        };

        return result;
    }

    onclose() {
        console.debug('stream.onclose');
        return super.onclose();
    }

    onpcvideo(ev) {
        console.debug('stream.onpcvideo');
        super.onpcvideo(ev);

        if (this.pcState !== WebSocket.CLOSED) {
            this.divMode = 'RTC';
        }
    }
}

customElements.define('video-stream', VideoStream);
