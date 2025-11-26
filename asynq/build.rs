use std::env;
use std::fs;
use std::path::Path;
fn main() -> Result<(), Box<dyn std::error::Error>> {
  // 当存在已提交的生成文件并且没有强制重生成标记时，跳过 protoc 调用
  // 允许使用环境变量 FORCE_PROTOC 或 PROTOC_REGENERATE 强制重生成（本地开发时用）
  let generated = Path::new("src/proto/asynq.rs");
  let force_regen = env::var("FORCE_PROTOC").is_ok() || env::var("PROTOC_REGENERATE").is_ok();
  if generated.exists() && !force_regen {
    // 只在 build.rs 自身变化时重跑（避免每次依赖构建都跑 prost）
    println!("cargo:rerun-if-changed=build.rs");
    return Ok(());
  }
  fs::create_dir_all("src/proto")?;
  let mut config = prost_build::Config::new();
  config.skip_debug(["."]);
  config.out_dir("src/proto");
  config.compile_protos(&["proto/asynq.proto"], &["proto/"])?;
  println!("cargo:rerun-if-changed=proto/asynq.proto");
  Ok(())
}