use axum::extract::State;
use axum::http::header::AUTHORIZATION;
use axum::http::{HeaderMap, StatusCode};
use subtle::ConstantTimeEq;

use crate::state::AppState;

fn valid_management_bearer(headers: &HeaderMap, state: &AppState) -> bool {
    let Some(expected) = state.management_token.as_deref() else {
        return false;
    };
    let mut values = headers.get_all(AUTHORIZATION).iter();
    let (Some(value), None) = (values.next(), values.next()) else {
        return false;
    };
    let Some((scheme, token)) = value.to_str().ok().and_then(|value| value.split_once(' ')) else {
        return false;
    };
    scheme.eq_ignore_ascii_case("bearer")
        && token.len() == expected.len()
        && bool::from(token.as_bytes().ct_eq(expected.as_bytes()))
}

pub(crate) async fn management_check(State(state): State<AppState>, headers: HeaderMap) -> StatusCode {
    if state.management_token.is_none() {
        StatusCode::SERVICE_UNAVAILABLE
    } else if valid_management_bearer(&headers, &state) {
        StatusCode::NO_CONTENT
    } else {
        StatusCode::UNAUTHORIZED
    }
}
