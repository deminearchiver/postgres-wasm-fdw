#[allow(warnings)]
mod bindings;
use serde_json::Value as JsonValue;

use bindings::{
  exports::supabase::wrappers::routines::Guest,
  supabase::wrappers::{
    http, time,
    types::{Cell, Context, FdwError, FdwResult, OptionsType, Row, TypeOid},
    utils,
  },
};
use url::Url;

#[derive(Debug)]
struct ExampleFdw {
  database_url: Url,
  auth_token: String,
  src_rows: Vec<JsonValue>,
  src_idx: usize,
}

// pointer for the static FDW instance
static mut INSTANCE: *mut ExampleFdw = std::ptr::null_mut::<ExampleFdw>();

impl ExampleFdw {
  fn init_instance(
    database_url: Url,
    auth_token: String,
  ) {
    let instance = Self {
      database_url,
      auth_token,
      src_rows: Vec::new(),
      src_idx: 0,
    };
    unsafe {
      INSTANCE = Box::leak(Box::new(instance));
    }
  }

  fn this_mut() -> &'static mut Self {
    unsafe { &mut (*INSTANCE) }
  }
}

impl Guest for ExampleFdw {
  fn host_version_requirement() -> String {
    // semver expression for Wasm FDW host version requirement
    // ref: https://docs.rs/semver/latest/semver/enum.Op.html
    "^0.1.0".to_string()
  }

  fn init(context: &Context) -> FdwResult {
    let options = context.get_options(OptionsType::Server);
    // this.base_url = opts.require_or("database_url", "https://api.github.com");

    let database_url = Url::parse(
      &options.require("database_url")?
    ).map_err(
      |error| format!("ParseError: {}\n\ndatabase_url must be of the form: https://[databaseName]-[organizationName].turso.io", error)
    )?;
    let auth_token = options.require("auth_token")?;


    Self::init_instance(database_url, auth_token);

    Ok(())
  }

  fn begin_scan(context: &Context) -> FdwResult {
    let this = Self::this_mut();

    let api_url = this.database_url.join("/v2/pipeline").unwrap();

    let headers: Vec<(String, String)> =
    vec![
      (
        "user-agent".into(),
        "Turso FDW".into(),
      ),
      (
        "authorization".into(),
        format!("Bearer {}", this.auth_token),
      )
    ];

    let request = http::Request {
      method: http::Method::Get,
      url: api_url.to_string(),
      headers,
      body: String::default(),
    };
    let response = http::get(&request)?;
    let response_json: JsonValue = serde_json::from_str(&response.body).map_err(|e| e.to_string())?;

    this.src_rows = response_json
      .as_array()
      .map(|v| v.to_owned())
      .expect("response should be a JSON array");

    utils::report_info(&format!("We got response array length: {}", this.src_rows.len()));

    Ok(())
  }

  fn iter_scan(ctx: &Context, row: &Row) -> Result<Option<u32>, FdwError> {
    let this = Self::this_mut();

    if this.src_idx >= this.src_rows.len() {
      return Ok(None);
    }

    let src_row = &this.src_rows[this.src_idx];
    for tgt_col in ctx.get_columns() {
      let tgt_col_name = tgt_col.name();
      let src = src_row
      .as_object()
      .and_then(|v| v.get(&tgt_col_name))
      .ok_or(format!("source column '{}' not found", tgt_col_name))?;
      let cell = match tgt_col.type_oid() {
        TypeOid::Bool => src.as_bool().map(Cell::Bool),
        TypeOid::String => src.as_str().map(|v| Cell::String(v.to_owned())),
        TypeOid::Timestamp => {
          if let Some(s) = src.as_str() {
            let ts = time::parse_from_rfc3339(s)?;
            Some(Cell::Timestamp(ts))
          } else {
            None
          }
        }
        TypeOid::Json => src.as_object().map(|_| Cell::Json(src.to_string())),
        _ => {
          return Err(format!(
            "column {} data type is not supported",
            tgt_col_name
          ));
        }
      };

      row.push(cell.as_ref());
    }

    this.src_idx += 1;

    Ok(Some(0))
  }

  fn re_scan(_ctx: &Context) -> FdwResult {
    Err("re_scan on foreign table is not supported".to_owned())
  }

  fn end_scan(_ctx: &Context) -> FdwResult {
    let this = Self::this_mut();
    this.src_rows.clear();
    Ok(())
  }

  fn begin_modify(_ctx: &Context) -> FdwResult {
    Err("modify on foreign table is not supported".to_owned())
  }

  fn insert(_ctx: &Context, _row: &Row) -> FdwResult {
    Ok(())
  }

  fn update(_ctx: &Context, _rowid: Cell, _row: &Row) -> FdwResult {
    Ok(())
  }

  fn delete(_ctx: &Context, _rowid: Cell) -> FdwResult {
    Ok(())
  }

  fn end_modify(_ctx: &Context) -> FdwResult {
    Ok(())
  }
}

bindings::export!(ExampleFdw with_types_in bindings);
