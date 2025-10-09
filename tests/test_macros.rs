//! Tests for macro functionality

#[cfg(feature = "macros")]
mod tests {
    use asynq::{
        error::Result,
        register_async_handlers, register_handlers,
        serve_mux::ServeMux,
        server::Handler,
        task::Task,
        task_handler, task_handler_async,
    };

    // Define test handlers using macros
    #[task_handler("test:sync")]
    fn test_sync_handler(task: Task) -> Result<()> {
        assert_eq!(task.get_type(), "test:sync");
        Ok(())
    }

    #[task_handler_async("test:async")]
    async fn test_async_handler(task: Task) -> Result<()> {
        assert_eq!(task.get_type(), "test:async");
        Ok(())
    }

    #[task_handler("test:sync2")]
    fn test_sync_handler2(_task: Task) -> Result<()> {
        Ok(())
    }

    #[task_handler_async("test:async2")]
    async fn test_async_handler2(_task: Task) -> Result<()> {
        Ok(())
    }

    #[tokio::test]
    async fn test_task_handler_macro() {
        // Create a ServeMux
        let mut mux = ServeMux::new();

        // Register handlers using macros
        register_handlers!(mux, test_sync_handler);
        register_async_handlers!(mux, test_async_handler);

        // Test sync handler
        let task = Task::new("test:sync", b"test payload").unwrap();
        let result = mux.process_task(task).await;
        assert!(result.is_ok());

        // Test async handler
        let task = Task::new("test:async", b"test payload").unwrap();
        let result = mux.process_task(task).await;
        assert!(result.is_ok());

        // Test non-existent handler
        let task = Task::new("test:nonexistent", b"test payload").unwrap();
        let result = mux.process_task(task).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_multiple_handlers_registration() {
        let mut mux = ServeMux::new();

        // Register multiple handlers at once
        register_handlers!(mux, test_sync_handler, test_sync_handler2);
        register_async_handlers!(mux, test_async_handler, test_async_handler2);

        // Test all handlers
        let task1 = Task::new("test:sync", b"payload").unwrap();
        assert!(mux.process_task(task1).await.is_ok());

        let task2 = Task::new("test:sync2", b"payload").unwrap();
        assert!(mux.process_task(task2).await.is_ok());

        let task3 = Task::new("test:async", b"payload").unwrap();
        assert!(mux.process_task(task3).await.is_ok());

        let task4 = Task::new("test:async2", b"payload").unwrap();
        assert!(mux.process_task(task4).await.is_ok());
    }

    #[test]
    fn test_pattern_constants_exist() {
        // Verify that the pattern constants are created by the macros
        assert_eq!(__test_sync_handler_PATTERN, "test:sync");
        assert_eq!(__test_async_handler_PATTERN, "test:async");
        assert_eq!(__test_sync_handler2_PATTERN, "test:sync2");
        assert_eq!(__test_async_handler2_PATTERN, "test:async2");
    }
}
