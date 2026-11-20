//! A blocking client for the daemon's control API (see [`crate::api`]).

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};

use anyhow::{Context, bail};
use serde_json::Value;

use crate::api::{self, Call, LogChunk, Request, Response, Status};

pub fn socket_path() -> anyhow::Result<PathBuf> {
    Ok(crate::state_dir()?.join("run").join(api::SOCKET_NAME))
}

pub struct Client {
    reader: BufReader<UnixStream>,
    writer: UnixStream,
    next_id: u64,
}

impl Client {
    pub fn connect(path: &Path) -> std::io::Result<Client> {
        let s = UnixStream::connect(path)?;
        Ok(Client {
            writer: s.try_clone()?,
            reader: BufReader::new(s),
            next_id: 1,
        })
    }

    /// Connect to this home's daemon, if one is running.
    pub fn connect_default() -> anyhow::Result<Client> {
        let path = socket_path()?;
        Client::connect(&path).with_context(|| {
            format!(
                "the katamaran daemon is not running ({}); start it with `katamaran start`",
                path.display()
            )
        })
    }

    fn send(&mut self, call: &Call) -> anyhow::Result<u64> {
        let id = self.next_id;
        self.next_id += 1;
        let mut line = serde_json::to_vec(&Request::new(id, call))?;
        line.push(b'\n');
        self.writer.write_all(&line)?;
        Ok(id)
    }

    fn read_line(&mut self) -> anyhow::Result<Value> {
        let mut line = String::new();
        if self.reader.read_line(&mut line)? == 0 {
            bail!("the daemon closed the connection");
        }
        Ok(serde_json::from_str(&line)?)
    }

    pub fn call(&mut self, call: &Call) -> anyhow::Result<Value> {
        let id = self.send(call)?;
        let resp: Response = serde_json::from_value(self.read_line()?)?;
        if resp.id != id {
            bail!("response for request {} instead of {id}", resp.id);
        }
        match (resp.result, resp.error) {
            (_, Some(e)) => Err(e.into()),
            (Some(v), None) => Ok(v),
            (None, None) => Ok(Value::Null),
        }
    }

    /// Like [`Client::call`], passing the notifications that arrive before
    /// the response to `on_note` as (method, params).
    pub fn call_notified(
        &mut self,
        call: &Call,
        on_note: &mut dyn FnMut(&str, &Value),
    ) -> anyhow::Result<Value> {
        let id = self.send(call)?;
        loop {
            let v = self.read_line()?;
            if v.get("id").is_none()
                && let Some(m) = v.get("method").and_then(Value::as_str)
            {
                on_note(m, &v["params"]);
                continue;
            }
            let resp: Response = serde_json::from_value(v)?;
            if resp.id != id {
                bail!("response for request {} instead of {id}", resp.id);
            }
            return match (resp.result, resp.error) {
                (_, Some(e)) => Err(e.into()),
                (Some(v), None) => Ok(v),
                (None, None) => Ok(Value::Null),
            };
        }
    }

    pub fn status(&mut self) -> anyhow::Result<Status> {
        Ok(serde_json::from_value(self.call(&Call::Status)?)?)
    }

    /// Follow a log: `on_chunk` gets the tail, then every addition, until it
    /// returns false or the daemon goes away.
    pub fn follow(
        &mut self,
        params: api::LogsParams,
        mut on_chunk: impl FnMut(&LogChunk) -> bool,
    ) -> anyhow::Result<()> {
        let first: LogChunk = serde_json::from_value(self.call(&Call::Logs(api::LogsParams {
            follow: true,
            ..params
        }))?)?;
        if !on_chunk(&first) {
            return Ok(());
        }
        loop {
            let v = match self.read_line() {
                Ok(v) => v,
                Err(_) => return Ok(()),
            };
            if v.get("method").and_then(Value::as_str) == Some("logs.data") {
                let chunk: LogChunk = serde_json::from_value(v["params"].clone())?;
                if !on_chunk(&chunk) {
                    return Ok(());
                }
            }
        }
    }
}
