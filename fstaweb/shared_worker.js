/**
 * SharedWorker: 負責全域連線管理與資料快取 (L1)
 * gemini-helpme: 
 * 1. 處理多分頁連線 (onconnect)。
 * 2. 實作 API 請求合併 (Request Joining)。
 * 3. 統一管理與 Hub Server (8081) 的通訊。
 */

const connections = [];
const cacheL1 = new Map(); // 簡單的記憶體快取，可進階對接 IndexDB

onconnect = function (e) {
    const port = e.ports[0];
    connections.push(port);

    port.onmessage = async function (event) {
        const { type, payload } = event.data;

        switch (type) {
            case 'FETCH_DATA':
                // gemini-helpme: 檢查快取或發起請求
                const result = await handleFetchData(payload);
                port.postMessage({ type: 'DATA_RESULT', payload: result });
                break;
            
            case 'BROADCAST_STATUS':
                // gemini-helpme: 向所有分頁廣播狀態 (例如 Syncing 提示)
                broadcast({ type: 'STATUS_UPDATE', payload });
                break;
        }
    };

    port.start();
};

async function handleFetchData({ symbol, year, key }) {
    const qs = `${symbol}-${year}`;
    if (cacheL1.has(qs)) return cacheL1.get(qs);

    try {
        // 呼叫您的 Hub Server API
        const response = await fetch(`http://localhost:8081/dapi?${qs}`);
        const data = await response.json();
        
        if (data.status === "Success") {
            cacheL1.set(qs, data.data);
        }
		data.key=key;
        return data;
    } catch (err) {
        return { status: "Error", message: err.message, key:key };
    }
}

function broadcast(msg) {
    connections.forEach(port => port.postMessage(msg));
}
