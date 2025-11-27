// static/app.js
(function(){
  //helpers
  function $(sel, root=document){ return root.querySelector(sel); }
  function clamp(n, lo, hi){ return Math.min(Math.max(n, lo), hi); }

  // Global variable to store current lot capacity 
  let currentLotCapacity = 0;

  //PURPOSE: update the donut chart and text values based on live occupancy data
  function renderDonut(root, total, occupied){
    const available = Math.max(total - occupied, 0);
    const pct = total > 0 ? clamp((occupied / total) * 100, 0, 100) : 0; 
    
    let judgement = "EMPTY"; //status labels
    if (pct === 100)      judgement = "FULL";
    else if (pct >= 90)   judgement = "ALMOST FULL";
    else if (pct >= 70)   judgement = "BUSY";
    else if (pct >= 40)   judgement = "MODERATE";
    else if (pct >= 10)   judgement = "LIGHT";
    else if (pct > 0)     judgement = "VERY LIGHT";

    //quering necessary elements
    const occ = root.querySelector(".donut-occ");
    const free = root.querySelector(".donut-free");
    const totalEl = root.querySelector("#donut-total");
    const availEl = root.querySelector("#available");
    const judgeEl = root.querySelector("#judgement");

    //radius and circumference
    const R = 40, C = 2 * Math.PI * R;
    const occLen  = (pct / 100) * C; //length of occupied stroke
    const freeLen = C - occLen; //length of free stroke

    occ.style.strokeDasharray  = `${occLen} ${C}`;
    occ.style.strokeDashoffset = 0;
    free.style.strokeDasharray  = `${occLen} ${freeLen}`;
    free.style.strokeDashoffset = 0;

    totalEl.textContent = String(total);
    availEl.textContent = String(available);
    judgeEl.textContent = judgement;
  }

  //PURPOSE: convert hourly metrics from the API into an SVG-based line graph
  function renderOccupancyChart(root, points, capacity){
    const svg = root.querySelector("#occ-chart");
    if(!svg || points.length === 0 || capacity === 0) { //data availability
        if(svg) svg.innerHTML = '<text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" style="font-size: 14px; fill: #666;">No historical data available.</text>';
        return;
    }
    
    //dimensions and padding
    const W=320, H=200, padL=36, padR=10, padT=16, padB=44; 
    const cw = W - padL - padR, ch = H - padT - padB;

    const times = points.map(p => p.time);
    const n = points.length;

    // p.pct is the average count from the DB
    const percentagePoints = points.map(p => ({
    time: p.time,
    // p.pct is already a percentage from DB
    pct: clamp(p.pct, 0, 100)
  }));

    //mapping data index -> x position
    function x(i){ return padL + (i/(n-1))*cw; }
    //mapping occupancy % -> y position
    function y(pct){ return padT + (100 - pct)/100 * ch; }

    // grid lines (20%,40%,60%,80%)
    const gridY = [20,40,60,80];
    const gridLines = gridY.map(g => {
      const gy = y(g);
      return `<line class="chart-grid" x1="${padL}" y1="${gy}" x2="${padL+cw}" y2="${gy}"/>
              <text class="chart-tick-label" x="${padL - 5}" y="${gy + 4}" text-anchor="end">${g}%</text>`;
    }).join("");
    //label every 3rd point (to avoid overcrowding)
    const ticks = percentagePoints.map((p,i)=>{
      const xi = x(i);
      const y0 = padT + ch;
      if (i % 3 === 0) {
        return `
          <line class="chart-tick-line" x1="${xi}" y1="${y0}" x2="${xi}" y2="${y0+4}"/>
          <text class="chart-tick" transform="translate(${xi-2},${y0+32}) rotate(-90)">${p.time}</text>
        `;
      } else {
        return '';
      }
    }).join("");

    //constructing line path
    const pathD = percentagePoints.map((p,i)=>`${i?'L':'M'} ${x(i)} ${y(p.pct)}`).join(" ");
    const path = `<path class="chart-line" d="${pathD}" fill="none" />`;

    //writing into the SVG container
    svg.innerHTML = `
      <rect x="0" y="0" width="${W}" height="${H}" fill="none" />
      <g>${gridLines}</g>
      <g>${ticks}</g>
      <g>${path}</g>
      <line x1="${padL}" y1="${padT + ch}" x2="${padL+cw}" y2="${padT + ch}" style="stroke: #333; stroke-width: 1;" />
      <line x1="${padL}" y1="${padT}" x2="${padL}" y2="${padT + ch}" style="stroke: #333; stroke-width: 1;" />
    `;
  }

//PURPOSE: injecting rows into recent activities table
function renderActivityTable(root, rows){
  const tbody = root.querySelector("#activity-table tbody");
  if(!tbody) return;
  
  tbody.innerHTML = rows.map(r => {
    //Ensuring data fields are strings 
    const date = r.date || 'N/A';
    const time = r.time || 'N/A';
    const event = r.event || 'N/A';
    
    return `
      <tr>
        <td>${date}</td>
        <td>${time}</td>
        <td>${event}</td>
      </tr>
    `;
  }).join("");
  
  if (rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="3" style="text-align: center; color: #999;">No recent activity found.</td></tr>`;
  }
}

//PURPOSE: Fetching the full analytics for a selected lot
async function fetchLotDetails(lotId, capacity) {
    const picker = $(".lot-picker");
    const avgDurEl  = $("#avg-duration", picker);
    const avgOccEl  = $("#avg-occ", picker);
    const statusEl  = $("#lot-error", picker);

    try {
        statusEl.textContent = "Loading details...";
        //API call to return historical data, recent events, hourly metrics and forecast
        const res = await fetch(`/api/lot_details/${lotId}`);
        const data = await res.json();

        const currentLotEntry = JSON.parse(
            picker.getAttribute('data-lots-cache')
        ).find(l => l.lot_id === lotId);

        //current hour summary
        const avgOccPct = Math.round(data.current_avg_occ || 0);  // %
        const avgDurMin = Math.round(data.current_avg_duration || 0); // minutes

        if (avgOccEl)
            avgOccEl.textContent = `${avgOccPct}%`;

        if (avgDurEl)
            avgDurEl.textContent = `${Math.floor(avgDurMin/60)}h ${avgDurMin%60}m`;

        //real-time donut
        renderDonut(
            picker,
            currentLotEntry.capacity,
            currentLotEntry.current_occupancy
        );

        //historical chart (time graph HH->occupancy%)
        renderOccupancyChart(picker, data.historical_metrics, capacity);

        //recent activities table
        renderActivityTable(picker, data.activities);

        statusEl.textContent = "";

        //predictions
        const predOccEl = $("#pred-occ", picker);
        const predDepEl = $("#pred-dep", picker);
        
        if (data.forecast) {
          const pct = data.forecast.predicted_occupancy_pct;
          const occPct = pct != null ? Math.round(pct * 100) : "—";
          predOccEl.textContent = `${occPct}%`;
          predDepEl.textContent = data.forecast.predicted_num_departures ?? "—";
        } else {
          predOccEl.textContent = "—";
          predDepEl.textContent = "—";
        }

    } catch (e) {
        console.error("Failed to load lot details:", e);
        statusEl.textContent = "Failed to load details or historical data.";
        renderOccupancyChart(picker, [], capacity);
        renderActivityTable(picker, []);
    }
}


//PURPOSE: initialization for each campus dashboard
async function initCampus(){
    const picker = $(".lot-picker");
    if(!picker) return; 

    const campus = picker.getAttribute("data-campus");
    const menu   = $("#lot-menu", picker);
    const title  = $("#selected-lot", picker);
    const lotNameEl = $(".lot-name", title);
    const statusEl  = $("#lot-error", picker);
    
    let data;
    try{
        statusEl.textContent = "Fetching current lot statuses...";
        //loading all lots for this campus
        const res = await fetch(`/api/lots/${campus}`);
        data = await res.json();
        if(!data.lots || !Array.isArray(data.lots)) throw new Error("Bad payload");
        statusEl.textContent = "";
    }catch(e){
        console.error("Initial load failed:", e);
        statusEl.textContent = "Failed to load lots from database.";
        return;
    }
    
    // Cache the lot data for later use
    picker.setAttribute('data-lots-cache', JSON.stringify(data.lots));
    // filling dropdown
    menu.innerHTML = data.lots
        .map(l => `<li role="option" data-id="${l.lot_id}" data-total="${l.capacity}">${l.lot_id} (${l.campus_name})</li>`)
        .join("");


    //PURPOSE: changing handler when user selects a new lot
    function setLotByName(lotId){
        const entry = data.lots.find(l => l.lot_id === lotId) || data.lots[0];
        if (!entry) {
            statusEl.textContent = "Error: Lot details not found.";
            return;
        }
        lotNameEl.textContent = entry.lot_id;
        currentLotCapacity = entry.capacity;
        
        [...menu.children].forEach(li => {
            li.setAttribute("aria-selected", li.dataset.id === entry.lot_id ? "true" : "false");
        });//Marking selected

        renderDonut(picker, entry.capacity, entry.current_occupancy);
        fetchLotDetails(entry.lot_id, entry.capacity);
    }
    //PURPOSE: dropdown open/close logic
    function toggle(open){
        const isOpen = menu.style.display === "block";
        const next = (open === undefined) ? !isOpen : !!open;
        menu.style.display = next ? "block" : "none";
        title.setAttribute("aria-expanded", String(next));
    }

    title.addEventListener("click", ()=> toggle());
    title.addEventListener("keydown", (e)=>{
        if(e.key === "Enter" || e.key === " "){
            e.preventDefault();
            toggle();
        }
    });

    menu.addEventListener("click", (e)=>{
        const li = e.target.closest("li");
        if(!li) return;
        setLotByName(li.dataset.id); 
        toggle(false);
    });

    window.addEventListener("click", (e)=>{
        if(!title.contains(e.target) && !menu.contains(e.target)) toggle(false);
    });
    if (data.lots.length > 0) {
        setLotByName(data.lots[0].lot_id);
    }
}

  //setting up the entire dashboard and logic
  addEventListener("DOMContentLoaded", ()=>{
    initCampus();
    //initLogForm(); 
  });
})();