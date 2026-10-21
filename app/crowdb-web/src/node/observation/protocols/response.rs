// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Validate the bounded S3 `ListBuckets` XML response.

pub(super) fn list_buckets_body(bytes: &[u8]) -> bool {
    use quick_xml::events::Event;
    let mut reader = quick_xml::Reader::from_reader(bytes);
    let mut depth = 0usize;
    let mut root = false;
    let mut buckets = false;
    loop {
        match reader.read_event() {
            Ok(Event::Start(element)) => {
                if depth == 0 {
                    if root || element.local_name().as_ref() != b"ListAllMyBucketsResult" {
                        return false;
                    }
                    root = true;
                }
                if depth == 1 && element.local_name().as_ref() == b"Buckets" {
                    buckets = true;
                }
                depth += 1;
            }
            Ok(Event::Empty(element)) => {
                if depth == 1 && element.local_name().as_ref() == b"Buckets" {
                    buckets = true;
                } else if depth == 0 {
                    return false;
                }
            }
            Ok(Event::End(_)) => {
                let Some(next) = depth.checked_sub(1) else {
                    return false;
                };
                depth = next;
            }
            Ok(Event::Eof) => return root && buckets && depth == 0,
            Ok(Event::DocType(_)) | Err(_) => return false,
            _ => {}
        }
    }
}
