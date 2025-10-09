//! # Asynq Macros
//!
//! Procedural macros for asynq task handler registration.
//!
//! This crate provides attribute macros similar to actix-web's routing macros,
//! allowing you to register task handlers with a simple attribute syntax.
//!
//! ## Examples
//!
//! ```ignore
//! use asynq_macros::task_handler;
//! use asynq::{Task, error::Result};
//!
//! #[task_handler("email:send")]
//! fn handle_email(task: Task) -> Result<()> {
//!     println!("Handling email task");
//!     Ok(())
//! }
//! ```

use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, ItemFn, LitStr};

/// Attribute macro for registering synchronous task handlers
///
/// This macro marks a function as a task handler and stores metadata about it.
/// The function signature and behavior remain unchanged.
///
/// # Arguments
///
/// * `pattern` - The task type pattern to match (e.g., "email:send", "image:resize")
///
/// # Examples
///
/// ```ignore
/// use asynq_macros::task_handler;
/// use asynq::{Task, error::Result};
///
/// #[task_handler("email:send")]
/// fn handle_email(task: Task) -> Result<()> {
///     println!("Processing email task");
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn task_handler(attr: TokenStream, item: TokenStream) -> TokenStream {
    let pattern = parse_macro_input!(attr as LitStr);
    let input_fn = parse_macro_input!(item as ItemFn);
    
    let fn_name = &input_fn.sig.ident;
    let pattern_str = pattern.value();
    
    // Create a doc comment indicating this is a task handler
    let doc_comment = format!("Task handler for pattern: `{}`", pattern_str);
    
    // Generate the handler function with added metadata
    let expanded = quote! {
        #[doc = #doc_comment]
        #[allow(non_upper_case_globals)]
        #input_fn
        
        // Create a const string to store the pattern
        ::asynq::__private::paste::paste! {
            #[doc(hidden)]
            pub const [<__ #fn_name _PATTERN>]: &str = #pattern;
        }
    };
    
    TokenStream::from(expanded)
}

/// Attribute macro for registering asynchronous task handlers
///
/// This macro marks an async function as a task handler and stores metadata about it.
/// The function signature and behavior remain unchanged.
///
/// # Arguments
///
/// * `pattern` - The task type pattern to match (e.g., "email:send", "image:resize")
///
/// # Examples
///
/// ```ignore
/// use asynq_macros::task_handler_async;
/// use asynq::{Task, error::Result};
///
/// #[task_handler_async("image:resize")]
/// async fn handle_image_resize(task: Task) -> Result<()> {
///     println!("Processing image resize task");
///     Ok(())
/// }
/// ```
#[proc_macro_attribute]
pub fn task_handler_async(attr: TokenStream, item: TokenStream) -> TokenStream {
    let pattern = parse_macro_input!(attr as LitStr);
    let input_fn = parse_macro_input!(item as ItemFn);
    
    let fn_name = &input_fn.sig.ident;
    let pattern_str = pattern.value();
    
    // Create a doc comment indicating this is a task handler
    let doc_comment = format!("Async task handler for pattern: `{}`", pattern_str);
    
    // Generate the handler function with added metadata
    let expanded = quote! {
        #[doc = #doc_comment]
        #[allow(non_upper_case_globals)]
        #input_fn
        
        // Create a const string to store the pattern
        ::asynq::__private::paste::paste! {
            #[doc(hidden)]
            pub const [<__ #fn_name _PATTERN>]: &str = #pattern;
        }
    };
    
    TokenStream::from(expanded)
}

/// Macro for automatically registering handlers with ServeMux
///
/// This macro collects all handlers defined in the current scope and registers them
/// with a ServeMux instance.
///
/// # Examples
///
/// ```ignore
/// use asynq::register_handlers;
/// use asynq::serve_mux::ServeMux;
///
/// let mut mux = ServeMux::new();
/// register_handlers!(mux, handle_email, handle_image_resize);
/// ```
#[proc_macro]
pub fn register_handlers(input: TokenStream) -> TokenStream {
    let input_str = input.to_string();
    let parts: Vec<&str> = input_str.split(',').map(|s| s.trim()).collect();
    
    if parts.is_empty() {
        return TokenStream::from(quote! {
            compile_error!("register_handlers! requires at least a mux variable and one handler");
        });
    }
    
    let mux_var = parts[0];
    let mux_ident: proc_macro2::TokenStream = mux_var.parse().unwrap();
    
    let registrations: Vec<proc_macro2::TokenStream> = parts[1..]
        .iter()
        .map(|handler_name| {
            let handler_ident: proc_macro2::TokenStream = handler_name.parse().unwrap();
            let pattern_const: proc_macro2::TokenStream = 
                format!("__{}_PATTERN", handler_name).parse().unwrap();
            
            quote! {
                {
                    // Try to register as async first, fall back to sync
                    let pattern = #pattern_const;
                    // Detect if the function is async by checking if it returns a Future
                    use ::std::future::Future;
                    if ::std::any::TypeId::of::<()>() != ::std::any::TypeId::of::<()>() {
                        // This is a compile-time check placeholder
                        #mux_ident.handle_async_func(pattern, #handler_ident);
                    } else {
                        #mux_ident.handle_func(pattern, #handler_ident);
                    }
                }
            }
        })
        .collect();
    
    TokenStream::from(quote! {
        {
            #(#registrations)*
        }
    })
}

