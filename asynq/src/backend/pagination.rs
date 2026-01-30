/// Pagination specifies the page size and page number for list operations.
/// 分页结构体，指定列表操作的页面大小和页面编号
#[derive(Debug, Clone, Copy)]
pub struct Pagination {
  /// Number of items in the page.
  /// 每页的项目数
  pub size: i64,
  /// Page number starting from zero.
  /// 从零开始的页面编号
  pub page: i64,
}

impl Pagination {
  /// Returns the start index for the current page.
  /// 返回当前页面的起始索引
  pub fn start(&self) -> i64 {
    self.size * self.page
  }
  /// Returns the stop index for the current page.
  /// 返回当前页面的结束索引
  pub fn stop(&self) -> i64 {
    self.size * self.page + self.size - 1
  }
}
impl Default for Pagination {
  fn default() -> Self {
    Pagination { size: 20, page: 0 }
  }
}
