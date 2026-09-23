use crowdb_access_iceberg::catalog::CatalogError;
use crowdb_access_iceberg::file::{MultipartRepository, MultipartSelection, MultipartSession, SelectedPart};
use quick_xml::events::Event;
use quick_xml::Reader;

const MAX_COMPLETE_XML_BYTES: usize = 2 * 1024 * 1024;
const MAX_COMPLETE_PARTS: usize = 10_000;
const S3_NAMESPACE: &[u8] = b"http://s3.amazonaws.com/doc/2006-03-01/";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletePart {
    pub number: u16,
    pub digest: [u8; 32],
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompleteSelection {
    parts: Vec<CompletePart>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
#[error("invalid multipart completion XML")]
pub struct CompleteRequestError;

#[derive(Debug, thiserror::Error)]
pub enum CompleteResolveError {
    #[error("multipart selection does not match durable parts")]
    InvalidPart,
    #[error("a nonfinal multipart part is smaller than 5 MiB")]
    EntityTooSmall,
    #[error(transparent)]
    Catalog(#[from] CatalogError),
}

impl CompleteSelection {
    /// Parses a bounded S3 `CompleteMultipartUpload` body. The caller must match
    /// each selected digest to the current durable part revision before freezing.
    /// # Errors
    /// Rejects malformed XML, extra fields and unordered or duplicate parts.
    pub fn parse(bytes: &[u8]) -> Result<Self, CompleteRequestError> {
        if bytes.is_empty() || bytes.len() > MAX_COMPLETE_XML_BYTES {
            return Err(CompleteRequestError);
        }
        let mut reader = Reader::from_reader(bytes);
        let mut state = State::Start;
        let mut parts = Vec::new();
        let mut number = None;
        let mut digest = None;
        loop {
            match reader.read_event().map_err(|_| CompleteRequestError)? {
                Event::Decl(_) if state == State::Start => {}
                Event::Start(event) if valid_attributes(state, &event)? => {
                    state = match (state, event.name().as_ref()) {
                        (State::Start, b"CompleteMultipartUpload") => State::Root,
                        (State::Root, b"Part") if parts.len() < MAX_COMPLETE_PARTS => State::Part,
                        (State::Part, b"PartNumber") if number.is_none() => State::Number,
                        (State::Part, b"ETag") if digest.is_none() => State::Etag,
                        _ => return Err(CompleteRequestError),
                    };
                }
                Event::Text(event) => match state {
                    State::Number if number.is_none() => {
                        let value: &[u8] = event.as_ref();
                        if value.is_empty() || !value.iter().all(u8::is_ascii_digit) {
                            return Err(CompleteRequestError);
                        }
                        number = Some(
                            std::str::from_utf8(value)
                                .map_err(|_| CompleteRequestError)?
                                .parse::<u16>()
                                .map_err(|_| CompleteRequestError)?,
                        );
                    }
                    State::Etag if digest.is_none() => digest = Some(parse_etag(&event)?),
                    State::Start | State::Root | State::Part | State::Done
                        if event.iter().all(u8::is_ascii_whitespace) => {}
                    _ => return Err(CompleteRequestError),
                },
                Event::End(event) => {
                    state = match (state, event.name().as_ref()) {
                        (State::Number, b"PartNumber") if number.is_some() => State::Part,
                        (State::Etag, b"ETag") if digest.is_some() => State::Part,
                        (State::Part, b"Part") => {
                            let number = number.take().ok_or(CompleteRequestError)?;
                            let digest = digest.take().ok_or(CompleteRequestError)?;
                            if number == 0
                                || number > 10_000
                                || parts
                                    .last()
                                    .is_some_and(|part: &CompletePart| part.number >= number)
                            {
                                return Err(CompleteRequestError);
                            }
                            parts.push(CompletePart { number, digest });
                            State::Root
                        }
                        (State::Root, b"CompleteMultipartUpload") if !parts.is_empty() => State::Done,
                        _ => return Err(CompleteRequestError),
                    };
                }
                Event::Eof if state == State::Done => return Ok(Self { parts }),
                _ => return Err(CompleteRequestError),
            }
        }
    }

    #[must_use]
    pub fn parts(&self) -> &[CompletePart] {
        &self.parts
    }

    /// Resolves the selected parts against one current durable session snapshot.
    /// # Errors
    /// Rejects missing, replaced or differently hashed parts and storage failures.
    pub async fn resolve(
        &self,
        repository: &MultipartRepository,
        session: &MultipartSession,
    ) -> Result<MultipartSelection, CompleteResolveError> {
        if self.parts.len() > usize::from(session.limits.max_parts) {
            return Err(CompleteResolveError::InvalidPart);
        }
        let mut selected = Vec::with_capacity(self.parts.len());
        for (index, requested) in self.parts.iter().enumerate() {
            let part = repository
                .part(session, requested.number)
                .await?
                .ok_or(CompleteResolveError::InvalidPart)?;
            if part.tree.digest != requested.digest {
                return Err(CompleteResolveError::InvalidPart);
            }
            if index + 1 < self.parts.len() && part.tree.length < 5 * 1024 * 1024 {
                return Err(CompleteResolveError::EntityTooSmall);
            }
            selected.push(SelectedPart {
                number: part.number,
                revision: part.revision,
                digest: part.tree.digest,
            });
        }
        MultipartSelection::new(selected).map_err(|_| CompleteResolveError::InvalidPart)
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum State {
    Start,
    Root,
    Part,
    Number,
    Etag,
    Done,
}

fn parse_etag(bytes: &[u8]) -> Result<[u8; 32], CompleteRequestError> {
    let hex = bytes
        .strip_prefix(b"\"")
        .and_then(|bytes| bytes.strip_suffix(b"\""))
        .ok_or(CompleteRequestError)?;
    if hex.len() != 64 {
        return Err(CompleteRequestError);
    }
    let mut digest = [0; 32];
    for (target, pair) in digest.iter_mut().zip(hex.chunks_exact(2)) {
        *target = (hex_digit(pair[0])? << 4) | hex_digit(pair[1])?;
    }
    Ok(digest)
}

fn hex_digit(byte: u8) -> Result<u8, CompleteRequestError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        _ => Err(CompleteRequestError),
    }
}

fn valid_attributes(
    state: State,
    event: &quick_xml::events::BytesStart<'_>,
) -> Result<bool, CompleteRequestError> {
    let mut attributes = event.attributes();
    let Some(attribute) = attributes.next() else {
        return Ok(true);
    };
    let attribute = attribute.map_err(|_| CompleteRequestError)?;
    Ok(state == State::Start
        && event.name().as_ref() == b"CompleteMultipartUpload"
        && attribute.key.as_ref() == b"xmlns"
        && attribute.value.as_ref() == S3_NAMESPACE
        && attributes.next().is_none())
}
