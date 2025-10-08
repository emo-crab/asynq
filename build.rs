fn main() -> Result<(), Box<dyn std::error::Error>> {
  // 生成 Protocol Buffer 代码
  println!("cargo:rerun-if-changed=build.rs");
  println!("cargo:rerun-if-changed=path/to/Cargo.lock");
  prost_build::Config::new()
    .skip_debug(["."])
    .out_dir("src/proto/")
    .compile_protos(&["proto/asynq.proto"], &["proto/"])?;
  Ok(())
}
