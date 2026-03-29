//! Embedded CSS styles for the Asynq Dashboard UI.

/// Minified CSS stylesheet injected into every rendered page.
pub(crate) const STYLE: &str = r#"
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,sans-serif;background:#f5f7f9;color:#24292e;min-height:100vh}
a{color:#0366d6;text-decoration:none}a:hover{text-decoration:underline}
nav{background:#24292e;color:#fff;display:flex;align-items:center;padding:0 24px;height:56px;gap:16px;position:sticky;top:0;z-index:100;box-shadow:0 1px 3px rgba(0,0,0,.3)}
nav .brand{font-size:18px;font-weight:700;letter-spacing:-.5px;color:#fff;display:flex;align-items:center;gap:8px}
nav .brand-icon{font-size:22px}
nav a.nav-link{color:#ccc;padding:8px 12px;border-radius:4px;transition:background .15s}
nav a.nav-link:hover,nav a.nav-link.active{background:#444;color:#fff;text-decoration:none}
.container{max-width:1280px;margin:0 auto;padding:24px 16px}
.card{background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.06);padding:20px;margin-bottom:16px;border:1px solid #e8ecf0}
.card-title{font-size:12px;color:#586069;font-weight:600;margin-bottom:6px;text-transform:uppercase;letter-spacing:.6px}
.card-value{font-size:26px;font-weight:700;color:#24292e}
.card-subtitle{font-size:12px;color:#999;margin-top:4px}
.grid{display:grid;gap:12px}
.grid-2{grid-template-columns:repeat(2,1fr)}
.grid-3{grid-template-columns:repeat(3,1fr)}
.grid-4{grid-template-columns:repeat(4,1fr)}
.grid-5{grid-template-columns:repeat(5,1fr)}
@media(max-width:1024px){.grid-5{grid-template-columns:repeat(3,1fr)}}
@media(max-width:768px){.grid-5,.grid-4,.grid-3,.grid-2{grid-template-columns:repeat(2,1fr)}}
@media(max-width:480px){.grid-5,.grid-4,.grid-3,.grid-2{grid-template-columns:1fr}}
.badge{display:inline-block;padding:2px 8px;border-radius:12px;font-size:12px;font-weight:500}
.badge-paused{background:#ffeef0;color:#d73a49}
.badge-running{background:#e6f4ea;color:#1e7e34}
.page-header{display:flex;align-items:center;justify-content:space-between;margin-bottom:20px;flex-wrap:wrap;gap:12px}
.page-header h1{font-size:24px;font-weight:700}
.breadcrumb{font-size:13px;color:#586069;margin-bottom:8px}
.breadcrumb a{color:#0366d6}
.tabs{display:flex;gap:4px;border-bottom:2px solid #e1e4e8;margin-bottom:20px;flex-wrap:wrap}
.tab{padding:8px 16px;border-radius:4px 4px 0 0;font-size:14px;font-weight:500;color:#586069;display:flex;align-items:center;gap:6px;transition:background .15s}
.tab a{color:inherit;text-decoration:none;display:flex;align-items:center;gap:6px}
.tab:hover{background:#f1f3f4}
.tab.active{color:#0366d6;border-bottom:2px solid #0366d6;margin-bottom:-2px}
.tab .count{background:#e1e4e8;border-radius:10px;padding:1px 6px;font-size:11px}
.state-chips{display:flex;flex-wrap:wrap;gap:8px;margin-bottom:20px}
.state-chip{display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:20px;font-size:13px;font-weight:500;border:1px solid #d1d5da;color:#24292e;background:#fff;transition:background .15s;text-decoration:none}
.state-chip:hover{background:#f6f8fa;text-decoration:none}
.state-chip-primary{border-color:#4379FF;color:#4379FF;background:#eef2ff}
.chip-count{background:#e1e4e8;border-radius:10px;padding:1px 6px;font-size:11px;color:#586069}
.state-chip-primary .chip-count{background:#c7d5ff;color:#1a3fcc}
.table-wrap{overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:14px}
thead{background:#f6f8fa}
th{padding:10px 12px;text-align:left;font-weight:600;font-size:12px;color:#586069;text-transform:uppercase;letter-spacing:.5px;border-bottom:2px solid #e1e4e8;white-space:nowrap}
th.text-right{text-align:right}
th.text-center{text-align:center}
td{padding:10px 12px;border-bottom:1px solid #e8ecf0;vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:#f9fafb}
.text-right{text-align:right}
.text-center{text-align:center}
.task-id{font-family:monospace;font-size:12px;color:#586069;max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.task-type{font-weight:500;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.err-msg{max-width:240px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;font-size:12px;color:#d73a49}
.payload-pre{font-family:monospace;font-size:11px;background:#fafafa;border:1px solid #e1e4e8;border-radius:4px;padding:6px 8px;max-width:360px;max-height:120px;overflow:auto;white-space:pre;margin:0}
.c-active{color:#2ea44f}.c-pending{color:#0366d6}.c-scheduled{color:#6f42c1}
.c-retry{color:#d73a49}.c-archived{color:#586069}.c-completed{color:#22863a}.c-aggregating{color:#e36209}
.btn{display:inline-flex;align-items:center;gap:4px;padding:6px 14px;border-radius:6px;font-size:13px;font-weight:500;cursor:pointer;border:1px solid;transition:background .15s,border-color .15s}
.btn-primary{background:#0366d6;border-color:#0366d6;color:#fff}.btn-primary:hover{background:#0256c7}
.btn-danger{background:#d73a49;border-color:#d73a49;color:#fff}.btn-danger:hover{background:#c62f3d}
.btn-secondary{background:#fff;border-color:#d1d5da;color:#24292e}.btn-secondary:hover{background:#f1f3f4}
.btn-warning{background:#e36209;border-color:#e36209;color:#fff}.btn-warning:hover{background:#c4540a}
.btn-success{background:#22863a;border-color:#22863a;color:#fff}.btn-success:hover{background:#1a6b2e}
.btn-sm{padding:4px 10px;font-size:12px}
.btn-group{display:flex;gap:6px;flex-wrap:wrap}
.empty{text-align:center;padding:40px;color:#586069}
.empty-icon{font-size:40px;margin-bottom:12px}
.section-title{font-size:13px;font-weight:600;color:#24292e;margin:20px 0 12px;text-transform:uppercase;letter-spacing:.6px}
.text-muted{color:#586069;font-size:13px}
.pagination{display:flex;gap:4px;align-items:center;justify-content:center;margin-top:20px}
.pager{padding:6px 12px;border-radius:4px;border:1px solid #e1e4e8;font-size:13px;background:#fff}
.pager:hover:not(.disabled):not(.active){background:#f1f3f4}
.pager.active{background:#0366d6;border-color:#0366d6;color:#fff}
.pager.disabled{color:#ccc}
form{display:inline}
"#;

/// Number of tasks returned per page in the task-list view.
pub(crate) const TASK_PAGE_SIZE: i64 = 20;
