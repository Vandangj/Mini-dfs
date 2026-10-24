/**
 * MiniDFS Cloud - Enterprise Distributed Storage & Analytics Mesh
 * Powers Cloud Drive, Analytics Studio (MapReduce), HPC Acceleration (MPI),
 * Node Mesh Management, and Audit Synchronization.
 */

const MiniDFSApp = {
    activeTab: 'tab-overview',
    pollInterval: null,
    systemState: null,
    currentUser: 'alice',
    allFiles: {},

    init() {
        console.log("Starting MiniDFS Cloud Platform...");
        this.bindEvents();
        this.fetchSystemOverview();

        // Real-time cluster telemetry polling every 2.5s
        this.pollInterval = setInterval(() => {
            this.fetchSystemOverview();
        }, 2500);
    },

    bindEvents() {
        // Navigation buttons
        document.querySelectorAll('.nav-btn').forEach(btn => {
            btn.addEventListener('click', () => {
                const targetTab = btn.getAttribute('data-tab');
                this.switchTab(targetTab, btn);
            });
        });

        // Manual refresh button
        const refreshBtn = document.getElementById('btn-manual-refresh');
        if (refreshBtn) {
            refreshBtn.addEventListener('click', () => {
                this.fetchSystemOverview();
            });
        }
    },

    switchTab(tabId, navButton) {
        this.activeTab = tabId;

        // Update nav active states
        document.querySelectorAll('.nav-btn').forEach(b => b.classList.remove('active'));
        if (!navButton) {
            navButton = document.querySelector(`.nav-btn[data-tab="${tabId}"]`);
        }
        if (navButton) navButton.classList.add('active');

        // Update view panes
        document.querySelectorAll('.tab-pane').forEach(p => p.classList.remove('active'));
        const activePane = document.getElementById(tabId);
        if (activePane) activePane.classList.add('active');

        // Update breadcrumb title
        const titleMap = {
            'tab-overview': 'Cluster Overview',
            'tab-drive': 'Cloud Drive (Distributed Files)',
            'tab-analytics': 'Big Data Analytics Studio',
            'tab-hpc': 'HPC Acceleration (MPI Engine)',
            'tab-mesh': 'Storage Node Mesh & Fault Tolerance',
            'tab-audit': 'Audit Trail & Clock Synchronization',
            'tab-settings': 'Consensus & Routing Settings'
        };
        const titleElem = document.getElementById('current-view-title');
        if (titleElem && titleMap[tabId]) {
            titleElem.textContent = titleMap[tabId];
        }

        this.renderCurrentView();
    },

    async fetchSystemOverview() {
        try {
            const resp = await fetch('/api/system/overview');
            if (!resp.ok) return;
            const data = await resp.json();
            this.systemState = data;
            this.updateHeaderAndStatus(data);
            this.renderCurrentView();
        } catch (err) {
            console.warn("Telemetry fetch error:", err);
        }
    },

    updateHeaderAndStatus(data) {
        if (!data) return;

        const nodes = data.fault_tolerance?.nodes || {};
        const aliveCount = Object.values(nodes).filter(n => n.is_alive).length;
        const totalNodes = Object.keys(nodes).length || 3;

        // Header pills
        const nodesPill = document.getElementById('header-nodes-alive');
        if (nodesPill) nodesPill.textContent = `${aliveCount} / ${totalNodes} Online`;

        const mode = data.ring_topology?.consistency_mode || 'QUORUM_STRONG';
        const modeLabel = (mode === 'QUORUM_STRONG') ? 'Quorum (W=2)' : (mode === 'EVENTUAL' ? 'Eventual' : 'Causal');
        const modePill = document.getElementById('header-consistency-mode');
        if (modePill) modePill.textContent = modeLabel;

        const syncOffset = data.berkeley_clocks?.last_berkeley_sync?.average_diff_ms ?? 0.0;
        const offsetPill = document.getElementById('header-sync-offset');
        if (offsetPill) offsetPill.textContent = `${syncOffset} ms`;

        // Sidebar status
        const clockTs = data.lamport_clock?.logical_time || 1;
        const sidebarClock = document.getElementById('sidebar-clock-ts');
        if (sidebarClock) sidebarClock.textContent = `Logical Clock: #${clockTs}`;

        const filesCount = data.master?.total_files || 0;
        const navFilesCount = document.getElementById('nav-files-count');
        if (navFilesCount) navFilesCount.textContent = filesCount;
        const ovFilesCount = document.getElementById('overview-files-count');
        if (ovFilesCount) ovFilesCount.textContent = `${filesCount} Files`;

        // Mesh status dot in sidebar
        const meshDot = document.getElementById('mesh-status-dot');
        if (meshDot) {
            meshDot.className = (aliveCount === totalNodes) ? 'status-dot green' : 'status-dot';
            meshDot.style.background = (aliveCount === totalNodes) ? 'var(--accent-green)' : 'var(--accent-rose)';
        }

        // RWLock status on Master
        const rwlock = data.rwlock || {};
        const rwlockStat = document.getElementById('overview-rwlock-stat');
        if (rwlockStat) {
            if (rwlock.writer_active) rwlockStat.textContent = 'Writing (Exclusive)';
            else if (rwlock.active_readers > 0) rwlockStat.textContent = `Reading (${rwlock.active_readers} active)`;
            else rwlockStat.textContent = 'RWLock (Idle)';
        }

        const ovLamport = document.getElementById('overview-lamport-ts');
        if (ovLamport) ovLamport.textContent = `#${clockTs}`;

        // Node Cards on Overview
        ['SN1', 'SN2', 'SN3'].forEach(nid => {
            const nodeInfo = nodes[nid] || {};
            const loadInfo = data.load_balancer?.nodes?.[nid] || {};
            const isAlive = nodeInfo.is_alive !== false;

            const card = document.getElementById(`overview-${nid.toLowerCase()}-card`);
            if (card) {
                card.style.opacity = isAlive ? '1' : '0.55';
                card.style.borderColor = isAlive ? 'var(--border-color)' : 'var(--accent-rose)';
            }

            const dot = document.getElementById(`${nid.toLowerCase()}-overview-dot`);
            if (dot) {
                dot.textContent = isAlive ? 'Healthy' : 'Crashed';
                dot.style.color = isAlive ? 'var(--accent-green)' : 'var(--accent-rose)';
            }

            const loadText = document.getElementById(`${nid.toLowerCase()}-overview-load`);
            if (loadText) loadText.textContent = isAlive ? `${loadInfo.load_score || 10.0}` : 'OFFLINE';

            const hbText = document.getElementById(`${nid.toLowerCase()}-overview-hb`);
            if (hbText) hbText.textContent = isAlive ? `${nodeInfo.seconds_since_last_heartbeat || 1.0}s ago` : 'Missed';
        });

        // Overview Activity Feed
        const activityFeed = document.getElementById('overview-activity-feed');
        if (activityFeed && data.master?.recent_rpc_calls) {
            activityFeed.innerHTML = data.master.recent_rpc_calls.slice(-5).reverse().map(call => `
                <div class="activity-item">
                    <i class="fa-solid fa-code-branch"></i>
                    <div>
                        <strong>${call.method}</strong> by <code>${call.caller}</code>
                        <span class="text-secondary">&bull; ${call.result}</span>
                    </div>
                    <span class="activity-time">${call.timestamp}</span>
                </div>
            `).join('');
        }
    },

    renderCurrentView() {
        if (!this.systemState) return;
        const data = this.systemState;

        if (this.activeTab === 'tab-drive') {
            this.renderDriveView(data);
        } else if (this.activeTab === 'tab-mesh') {
            this.renderMeshView(data);
        } else if (this.activeTab === 'tab-audit') {
            this.renderAuditView(data);
        } else if (this.activeTab === 'tab-settings') {
            this.renderSettingsView(data);
        }
    },

    // ==================== 1. CLOUD DRIVE ====================
    renderDriveView(data) {
        this.allFiles = data.master?.files || {};
        this.filterFiles();
    },

    filterFiles() {
        const query = (document.getElementById('drive-search-input')?.value || '').toLowerCase();
        const tbody = document.getElementById('drive-files-body');
        if (!tbody) return;

        const files = this.allFiles;
        let html = '';

        for (const [path, meta] of Object.entries(files)) {
            if (query && !path.toLowerCase().includes(query)) continue;

            const chunksStr = (meta.chunks || []).map(cid => {
                const loc = this.systemState?.ring_topology?.nodes;
                return `<span class="tag btn-secondary">${cid}</span>`;
            }).join(' ');

            html += `
                <tr>
                    <td>
                        <div style="display:flex; align-items:center; gap:10px;">
                            <i class="fa-solid fa-file-code text-accent" style="font-size:16px;"></i>
                            <strong>${meta.path}</strong>
                        </div>
                    </td>
                    <td><span class="badge-accent">${meta.owner}</span></td>
                    <td><code>${meta.permissions}</code></td>
                    <td>${meta.size_bytes} B</td>
                    <td>v${meta.version}</td>
                    <td>${chunksStr}</td>
                    <td>
                        <div style="display:flex; gap:6px;">
                            <button class="btn btn-sm btn-secondary" title="Preview File" onclick="MiniDFSApp.previewFile('${meta.path}')">
                                <i class="fa-solid fa-eye"></i>
                            </button>
                            <button class="btn btn-sm btn-secondary" title="Download" onclick="MiniDFSApp.downloadFile('${meta.path}')">
                                <i class="fa-solid fa-download"></i>
                            </button>
                            <button class="btn btn-sm btn-danger" title="Delete" onclick="MiniDFSApp.deleteFile('${meta.path}')">
                                <i class="fa-solid fa-trash"></i>
                            </button>
                        </div>
                    </td>
                </tr>
            `;
        }

        tbody.innerHTML = html || '<tr><td colspan="7" class="text-muted">No files matching query found in namespace.</td></tr>';
    },

    onUserChange() {
        this.currentUser = document.getElementById('drive-current-user').value;
    },

    async previewFile(path) {
        try {
            const resp = await fetch('/api/rpc', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    jsonrpc: "2.0",
                    method: "read_file",
                    params: { path: path, user: this.currentUser },
                    id: Date.now()
                })
            });
            const res = await resp.json();
            const result = res.result || {};

            document.getElementById('preview-modal-title').innerHTML = `<i class="fa-solid fa-file-lines text-accent"></i> Preview: ${path}`;
            document.getElementById('preview-meta-bar').innerHTML = `
                <div class="metric-row">
                    <span>Owner: <strong>${result.metadata?.owner || 'alice'}</strong></span>
                    <span>Permissions: <code>${result.metadata?.permissions || 'rwxr-xr--'}</code></span>
                    <span>Version: <strong>v${result.metadata?.version || 1}</strong></span>
                </div>
            `;
            document.getElementById('preview-modal-content').textContent = result.content || result.error || 'Empty file';
            document.getElementById('preview-modal').classList.add('active');
        } catch (err) {
            alert(`Error reading file: ${err.message}`);
        }
    },

    closePreviewModal() {
        document.getElementById('preview-modal').classList.remove('active');
    },

    async downloadFile(path) {
        try {
            const resp = await fetch('/api/rpc', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    jsonrpc: "2.0",
                    method: "read_file",
                    params: { path: path, user: this.currentUser },
                    id: Date.now()
                })
            });
            const res = await resp.json();
            const content = res.result?.content || '';

            const filename = path.split('/').pop() || 'download.txt';
            const blob = new Blob([content], { type: 'text/plain;charset=utf-8' });
            const url = URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.href = url;
            a.download = filename;
            a.click();
            URL.revokeObjectURL(url);
        } catch (err) {
            alert(`Download failed: ${err.message}`);
        }
    },

    async deleteFile(path) {
        if (!confirm(`Are you sure you want to delete '${path}' from the distributed cluster?`)) return;
        try {
            await fetch('/api/rpc', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    jsonrpc: "2.0",
                    method: "delete_file",
                    params: { path: path, user: this.currentUser },
                    id: Date.now()
                })
            });
            this.fetchSystemOverview();
        } catch (err) {
            alert(`Delete failed: ${err.message}`);
        }
    },

    openUploadModal() {
        document.getElementById('upload-modal').classList.add('active');
    },

    closeUploadModal() {
        document.getElementById('upload-modal').classList.remove('active');
    },

    async submitFileUpload() {
        const path = document.getElementById('modal-upload-path').value;
        const user = document.getElementById('modal-upload-user').value;
        const perms = document.getElementById('modal-upload-perms').value;
        const content = document.getElementById('modal-upload-content').value;

        try {
            const resp = await fetch('/api/rpc', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({
                    jsonrpc: "2.0",
                    method: "create_file",
                    params: { path: path, user: user, content: content, permissions: perms },
                    id: Date.now()
                })
            });
            const res = await resp.json();
            this.closeUploadModal();
            this.fetchSystemOverview();
            this.switchTab('tab-drive');
        } catch (err) {
            alert(`Upload failed: ${err.message}`);
        }
    },

    // ==================== 2. ANALYTICS STUDIO ====================
    async runAnalyticsJob() {
        const jobType = document.getElementById('analytics-job-type').value;
        const customText = document.getElementById('analytics-custom-input').value;

        const resp = await fetch('/api/mapreduce/run', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ job_type: jobType, custom_text: customText })
        });
        const res = await resp.json();

        document.getElementById('analytics-job-id').textContent = `Job: ${res.job_id} (${res.timings_ms?.total_execution}ms)`;

        const t = res.timings_ms || {};
        document.getElementById('pipe-map-time').textContent = `${t.map_phase || 0} ms`;
        document.getElementById('pipe-shuffle-time').textContent = `${t.shuffle_phase || 0} ms`;
        document.getElementById('pipe-reduce-time').textContent = `${t.reduce_phase || 0} ms`;

        const tbody = document.getElementById('analytics-results-tbody');
        if (tbody && res.top_results) {
            tbody.innerHTML = res.top_results.map(r => `
                <tr>
                    <td><code>${r.key}</code></td>
                    <td><strong>${r.value}</strong></td>
                </tr>
            `).join('');
        }
    },

    // ==================== 3. HPC ACCELERATOR ====================
    async runHpcMatrixJob() {
        const dim = parseInt(document.getElementById('hpc-matrix-dim').value) || 8;
        const resp = await fetch('/api/matrix/multiply', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ dimension: dim })
        });
        const res = await resp.json();

        const box = document.getElementById('hpc-benchmark-box');
        if (box) box.style.display = 'block';

        document.getElementById('hpc-par-time').textContent = `${res.parallel_time_ms} ms`;
        document.getElementById('hpc-seq-time').textContent = `${res.sequential_time_ms} ms`;
        document.getElementById('hpc-speedup').textContent = `${res.speedup}x`;

        const pre = document.getElementById('hpc-matrix-preview');
        if (pre) pre.textContent = JSON.stringify(res, null, 2);
    },

    // ==================== 4. STORAGE NODE MESH ====================
    renderMeshView(data) {
        const nodes = data.fault_tolerance?.nodes || {};
        const lb = data.load_balancer?.nodes || {};

        ['SN1', 'SN2', 'SN3'].forEach(nid => {
            const nInfo = nodes[nid] || {};
            const isAlive = nInfo.is_alive !== false;

            const card = document.getElementById(`mesh-card-${nid.toLowerCase()}`);
            if (card) {
                card.style.opacity = isAlive ? '1' : '0.55';
                card.style.borderColor = isAlive ? 'var(--border-color)' : 'var(--accent-rose)';
            }

            const status = document.getElementById(`mesh-${nid.toLowerCase()}-status`);
            if (status) {
                status.textContent = isAlive ? 'HEALTHY' : 'CRASHED';
                status.style.color = isAlive ? 'var(--accent-green)' : 'var(--accent-rose)';
            }

            const hb = document.getElementById(`mesh-${nid.toLowerCase()}-hb`);
            if (hb) hb.textContent = isAlive ? `${nInfo.seconds_since_last_heartbeat}s` : 'DEAD (Timed out)';
        });

        // Version audit
        const audit = data.ring_topology?.audit || [];
        const tbody = document.getElementById('mesh-audit-tbody');
        if (tbody) {
            tbody.innerHTML = audit.map(a => `
                <tr>
                    <td><strong>${a.chunk_id}</strong></td>
                    <td>${a.primary_node}</td>
                    <td>${a.replica_node}</td>
                    <td>v${a.primary_version}</td>
                    <td>v${a.replica_version}</td>
                    <td><span class="tag ${a.synchronized ? 'btn-success' : 'btn-danger'}">${a.synchronized ? 'SYNCHRONIZED' : 'FAILOVER PROMOTED'}</span></td>
                </tr>
            `).join('');
        }
    },

    async crashNode(nodeId) {
        const resp = await fetch('/api/fault_tolerance/crash_node', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId })
        });
        const res = await resp.json();
        alert(`Storage Node ${nodeId} CRASHED! Automated Failover Triggered: ${JSON.stringify(res.failover)}`);
        this.fetchSystemOverview();
    },

    async reviveNode(nodeId) {
        const resp = await fetch('/api/fault_tolerance/revive_node', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ node_id: nodeId })
        });
        const res = await resp.json();
        alert(`Storage Node ${nodeId} REVIVED & RE-SYNCHRONIZED! Ring healed in ${res.sync_time_ms} ms`);
        this.fetchSystemOverview();
    },

    async triggerBullyElection() {
        const resp = await fetch('/api/elections/bully/trigger', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ initiator_id: 1 })
        });
        const res = await resp.json();
        alert(`Bully Election Complete! Elected Leader: ${res.elected_leader_name} (ID: ${res.elected_leader_id})`);
        this.fetchSystemOverview();
    },

    async triggerRingElection() {
        const resp = await fetch('/api/elections/ring/trigger', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ initiator_id: 1 })
        });
        const res = await resp.json();
        alert(`Token-Ring Election Complete! Elected Leader: Node ${res.elected_leader}`);
        this.fetchSystemOverview();
    },

    // ==================== 5. AUDIT & SYNCHRONIZATION ====================
    renderAuditView(data) {
        const rwlock = data.rwlock || {};
        const actR = document.getElementById('audit-active-readers');
        if (actR) actR.textContent = rwlock.active_readers ?? 0;
        const wAct = document.getElementById('audit-writer-active');
        if (wAct) wAct.textContent = rwlock.writer_active ? 'True' : 'False';

        // Berkeley clocks
        const bClocks = data.berkeley_clocks?.nodes || {};
        const bContainer = document.getElementById('audit-berkeley-clocks');
        if (bContainer) {
            bContainer.innerHTML = Object.entries(bClocks).map(([nid, info]) => `
                <div class="node-clock-card">
                    <div class="metric-row"><strong>${nid}</strong> <span>Drift: ${info.drift_rate_ms_sec} ms/s</span></div>
                    <div class="metric-row"><span>Physical Time:</span> <code>${info.physical_time_sec}s</code></div>
                    <div class="metric-row"><span>Applied Offset:</span> <strong>${info.offset_ms} ms</strong></div>
                </div>
            `).join('');
        }

        // Lamport event stream
        const lamport = data.lamport_clock || {};
        const lamportVal = document.getElementById('audit-lamport-val');
        if (lamportVal) lamportVal.textContent = `#${lamport.logical_time || 1}`;

        const stream = document.getElementById('audit-event-stream');
        if (stream && lamport.recent_events) {
            stream.innerHTML = lamport.recent_events.slice(-10).reverse().map(e => `
                <div class="trace-entry">
                    [Clock #${e.timestamp}] <strong>${e.type}</strong>: ${e.description} 
                    <span class="activity-time">${e.wall_time}</span>
                </div>
            `).join('');
        }
    },

    async runBerkeleySync() {
        const resp = await fetch('/api/clocks/berkeley/sync', { method: 'POST' });
        const res = await resp.json();
        alert(`Berkeley Sync Round Executed! Clock offset adjustments sent across nodes: ${JSON.stringify(res.adjustments_sent)}`);
        this.fetchSystemOverview();
    },

    async runCristianSync(clientId) {
        const resp = await fetch('/api/clocks/cristian/sync', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ client_id: clientId })
        });
        const res = await resp.json();
        alert(`Cristian's Sync for ${clientId} Complete! RTT: ${res.rtt_ms} ms, Adjustment: ${res.adjustment_ms} ms`);
        this.fetchSystemOverview();
    },

    async runAuditBenchmark() {
        const clients = parseInt(document.getElementById('audit-bench-clients').value) || 10;
        const reqs = parseInt(document.getElementById('audit-bench-reqs').value) || 20;

        try {
            const resp = await fetch('/api/rpc/benchmark', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ num_clients: clients, requests_per_client: reqs })
            });
            const res = await resp.json();

            const box = document.getElementById('audit-bench-results');
            if (box) box.style.display = 'block';

            document.getElementById('audit-throughput').textContent = `${res.throughput_req_per_sec} req/s`;
            document.getElementById('audit-lat').textContent = `${res.avg_latency_ms} ms`;
            document.getElementById('audit-total-reqs').textContent = res.total_requests;
            document.getElementById('audit-errors').textContent = res.errors;

            this.fetchSystemOverview();
        } catch (err) {
            alert(`Traffic benchmark failed: ${err.message}`);
        }
    },

    // ==================== 6. SETTINGS ====================
    renderSettingsView(data) {
        const mode = data.ring_topology?.consistency_mode || 'QUORUM_STRONG';
        const pol = data.load_balancer?.active_policy || 'LEAST_LOADED';

        const modeSel = document.getElementById('settings-consistency-mode');
        if (modeSel) modeSel.value = mode;

        const polSel = document.getElementById('settings-lb-policy');
        if (polSel) polSel.value = pol;
    },

    async updateSettingsConsistency() {
        const mode = document.getElementById('settings-consistency-mode').value;
        await fetch('/api/consistency/set_mode', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ mode: mode })
        });
        this.fetchSystemOverview();
    },

    async updateSettingsLb() {
        const pol = document.getElementById('settings-lb-policy').value;
        await fetch('/api/loadbalancer/set_policy', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ policy: pol })
        });
        this.fetchSystemOverview();
    },

    // ==================== 7. CONCEPTS ACADEMIC MODAL ====================
    openConceptsModal() {
        document.getElementById('concepts-modal').classList.add('active');
    },

    closeConceptsModal() {
        document.getElementById('concepts-modal').classList.remove('active');
    }
};

window.addEventListener('DOMContentLoaded', () => {
    MiniDFSApp.init();
});
