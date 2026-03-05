(()=>{

class Base {
	getDS(yr,yd)
	{	// format datecode 20260228 {{{
		if (yd) {
			yr=new Date(yr,0,1);
			yr.setDate(yd);
		} else if ('string'===typeof(yr)) yr=new Date(yr);
			return yr.getFullYear()*10000+(yr.getMonth()+1)*100+yr.getDate();
	}	// getDS }}}
}
class Data extends Base {
	constructor (sid) {
		super();
		this.SID=sid;
		this.Rs={};
		this.Vs={};
	}
	import (yc, rs) {
		this.Vs[yc]=0;
		rs.reduce((T,r,k) => {
			if (r) {
				if(T.Vs[yc]<k) T.Vs[yc]=k;
				T.Rs[this.getDS(yc,k)]=r;
			} return T;
		}, this);
	}	// import
	async fetch (sid, year) {
		// 格式: /dapi?2330.TW-2025
		const resp = await fetch(`/dapi?${sid}-${year}`);
		if (resp.status!==200) return {"error":"Bad Request"};
		return { "data": await resp.json() };
	}
	async get (fr, to) {
		let pending = false;
		for (let year=fr.getFullYear(); year<=to.getFullYear(); year++) {
			if (this.Vs[year]) continue;
			const res = await this.fetch(this.SID,year);
			if (res.data === "Pending")
				pending=true;
			else if (Array.isArray(res.data))
				this.import(year,res.data);
			else
				console.assert("Unrecognized type: ",typeof(res.data));
		}
		if (pending) return {"error":'<span class="pending">Task Scheduled. Please retry later.</span>'};
		fr = this.getDS(fr);
		to = this.getDS(to);
		return Object.keys(this.Rs).filter(
			(k) => k>=fr && k<=to
		).reduce((r,k) => {
			r[k]=this.Rs[k];
			return r;
		}, {});
	}	// get
}	// Data

window.SData=Data;

})();
