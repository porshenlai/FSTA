(()=>{

class Chart
{
	constructor (vp,ks)
	{	// {{{
		this.View = vp;
		vp.setAttribute("xmlns", "http://www.w3.org/2000/svg");
		vp.setAttribute("preserveAspectRatio", "none");
		vp.addEventListener('click',(evt) => {
			var s = evt.offsetX/vp.clientWidth;
			this.Cursor.setAttribute("x", parseInt(this.Canvas.K.querySelectorAll('rect').length*s)*10);
			this.printValues(s);
		});
		this.Canvas = {};
		["Z","K","A"].forEach((id)=>{
			let canvas=document.createElementNS("http://www.w3.org/2000/svg", "g");
			canvas.setAttribute("LAYER",id);
			vp.appendChild(canvas);
			this.Canvas[id]=canvas;
		});
		this.Cursor=document.createElementNS("http://www.w3.org/2000/svg", "rect");
		this.Cursor.setAttribute("x", 0);
		this.Cursor.setAttribute("y", 0);
		this.Cursor.setAttribute("width", 10);
		this.Cursor.setAttribute("height", 0);
		this.Cursor.setAttribute("fill", "white");
		this.Canvas.Z.appendChild(this.Cursor);

		this.allY = []; // 儲存所有 Y 座標以計算邊界
	}	// }}}

	clear (target)
	{	// clear canvas {{{
		if ((!target) || target==='K')
			while (this.Canvas.K.firstChild) this.Canvas.K.removeChild(this.Canvas.K.firstChild);
		if ((!target) || target==='A')
			while (this.Canvas.A.firstChild) this.Canvas.A.removeChild(this.Canvas.A.firstChild);
		if ((!target) || target==='T') {
			while (this.Canvas.Z.firstChild)
				this.Canvas.Z.removeChild(this.Canvas.Z.firstChild);
			this.Canvas.Z.appendChild(this.Cursor);
		}
	}	// }}}

	printValues (v, idx)
	{	// {{{
		console.log(JSON.stringify(v));
	}	// }}}

	plotTag (idx)
	{	// {{{
		let tag=document.createElementNS("http://www.w3.org/2000/svg", "line");
		let y=parseFloat(this.Cursor.getAttribute("y")),
			h=parseFloat(this.Cursor.getAttribute("height"));
		tag.setAttribute("x1", idx*10);
		tag.setAttribute("x2", idx*10);
		tag.setAttribute("y1", y);
		tag.setAttribute("y2", y+h);
		tag.setAttribute("stroke", "silver");
		this.Canvas.Z.appendChild(tag);
	}	// }}}

	plotLine (d=[0], c="#ff0000", w="2")
	{	// plot line chart {{{
		console.log(d);
		this.allY.push(...d);
		let points = d.map((val, i) => `${i * 10 + 5},${val}`).join(" ");
		let polyline = document.createElementNS("http://www.w3.org/2000/svg", "polyline");
		polyline.setAttribute("points", points);
		polyline.setAttribute("fill", "none");
		polyline.setAttribute("stroke", c);
		polyline.setAttribute("stroke-width", w);
		this.Canvas.A.appendChild(polyline);
	}	// }}}

	plotKC (d=[[1,2,3,0]], c=["#ff4444","#00ff44"])
	{	// plot candle chart {{{
		d.forEach((item, i) => {
			const [open, close, high, low] = item;
			this.allY.push(high, low);
			const x_base = i * 10;
			const isUp = close >= open;
			const color = isUp ? c[0] : c[1];

			// 影線
			let wick = document.createElementNS("http://www.w3.org/2000/svg", "line");
			wick.setAttribute("x1", x_base + 5); wick.setAttribute("y1", high);
			wick.setAttribute("x2", x_base + 5); wick.setAttribute("y2", low);
			wick.setAttribute("stroke", color);
			this.Canvas.K.appendChild(wick);

			// 實體
			let body = document.createElementNS("http://www.w3.org/2000/svg", "rect");
			body.setAttribute("x", x_base + 1);
			body.setAttribute("y", isUp ? open : close);
			body.setAttribute("width", "8");
			body.setAttribute("height", Math.abs(open - close) || 1);
			body.setAttribute("fill", color);
			this.Canvas.K.appendChild(body);
		});
	}	// }}}

	plotMACDHist (d, c=["#ff4444","#00ff44"])
	{	// 繪製 MACD 柱狀圖 {{{
		// d: 數值陣列 [h1, h2, h3...]
		// c: [正值顏色, 負值顏色]
		const g = document.createElementNS("http://www.w3.org/2000/svg", "g");
		g.setAttribute("class", "macd-hist");
	
		d.forEach((val, i) => {
			if (val === null) return;
			this.allY.push(val); // 記得加入邊界計算
		
			const rect = document.createElementNS("http://www.w3.org/2000/svg", "rect");
			const x = i * 10 + 2; // 寬度設為 6，留間距
			const height = Math.abs(val);
		
			rect.setAttribute("x", x);
			// 如果是正值，y 座標就是 val 本身(往上長)；負值則從 0 開始往下長
			// 註：這裡的 0 是指 MACD 的相對中軸，在 finalized() 時會處理
			rect.setAttribute("y", val > 0 ? 0 - val : 0); 
			rect.setAttribute("width", "6");
			rect.setAttribute("height", height);
			rect.setAttribute("fill", val > 0 ? c[0] : c[1]);
		
			g.appendChild(rect);
		});
		this.Canvas.A.appendChild(g);
	}	// }}}

	zoom (zm=1)
	{
		this.View.style.width=(zm*100)+'%';
		this.View.style.height='100%';
	}

	finalized (width="1")
	{	// 自動計算邊界並調整 viewBox {{{
		if (this.allY.length === 0) return;

		const minY = Math.min(...this.allY);
		const maxY = Math.max(...this.allY);
		const marginY = (maxY - minY) * 0.1; // 上下留 10% 空白
		
		// 計算總長度：假設 X 從 0 開始，最後一個點在 (allY.length / 2) * 10 
		// 這裡我們直接抓 CANVAS 的元素數量或資料長度來推算 X
		const totalElements = this.allY.length; 
		const widthX = (this.Canvas.K.querySelectorAll('rect').length) * 10;

		// 設定 SVG 的實際寬度，使其突出畫面產生滾動條
		this.View.style.width = widthX + "px";

		// viewBox: [minX, minY, width, height]
		// 注意：SVG 的 Y 軸通常是向下增加，如果你的數據 2100 比 2000 高，
		// 你可能需要調整 viewBox 的 Y 或在繪圖時做鏡像處理。
		// 這裡假設你的原始數據 y 值越大代表座標越低（符合 SVG 預設）。
		const vBoxY = minY - marginY;
		const vBoxH = (maxY - minY) + (marginY * 2);
		
		this.View.setAttribute("viewBox", `0 ${vBoxY} ${widthX} ${vBoxH}`);
		this.Cursor.setAttribute("y", minY);
		this.Cursor.setAttribute("height", maxY);
		
		console.log(`Finalized: Width=${widthX}, YRange=[${minY}, ${maxY}]`);
	}	// }}}

	test ()
	{	// 測試資料：模擬 100 天的走勢 {{{
		let mockData = [];
		let startPrice = 2000;
		for(let i=0; i<300; i++) {
			let open = startPrice + Math.random()*20 - 10;
			let close = open + Math.random()*40 - 20;
			let high = Math.max(open, close) + Math.random()*10;
			let low = Math.min(open, close) - Math.random()*10;
			mockData.push([open, close, high, low]);
			startPrice = close;
		}
		this.plotKC(mockData);
		this.plotLine("Middle",mockData.map(d=>(d[0]+d[1])/2), "#ffcc00", "1"); // 繪製中軸線
		this.finalized();
	}	// }}}
}

window.Chart=Chart;
})();
