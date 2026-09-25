use crowdb_access_iceberg::catalog::{Capabilities, FormatAction};
use hyper::Method;

pub(super) struct InstalledRoutes(pub u8);

impl InstalledRoutes {
    pub(super) const NAMESPACES: u8 = 1;
    pub(super) const TABLES: u8 = 2;
    pub(super) const WRITES: u8 = 4;
    pub(super) const CREDENTIALS: u8 = 8;

    fn contains(&self, flags: u8) -> bool {
        self.0 & flags == flags
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Route {
    Config,
    AdminMetrics,
    NamespaceList,
    NamespaceCreate,
    NamespaceLoad,
    NamespaceExists,
    NamespaceProperties,
    NamespaceDrop,
    TableList,
    TableCreate,
    TableLoad,
    TableExists,
    TableUpdate,
    TableDrop,
    TableRename,
    TableCredentials,
}

impl Route {
    const ADVERTISED: [Self; 14] = [
        Self::NamespaceList,
        Self::NamespaceLoad,
        Self::NamespaceExists,
        Self::NamespaceCreate,
        Self::NamespaceProperties,
        Self::NamespaceDrop,
        Self::TableList,
        Self::TableLoad,
        Self::TableExists,
        Self::TableCreate,
        Self::TableUpdate,
        Self::TableDrop,
        Self::TableRename,
        Self::TableCredentials,
    ];

    pub(super) fn classify(method: &Method, path: &str) -> Option<Self> {
        if path == "/_crowdb/metrics" {
            return (method == Method::GET).then_some(Self::AdminMetrics);
        }
        if path == "/v1/config" {
            return (method == Method::GET).then_some(Self::Config);
        }
        if path == "/v1/tables/rename" {
            return (method == Method::POST).then_some(Self::TableRename);
        }
        if path == "/v1/namespaces" {
            return match *method {
                Method::GET => Some(Self::NamespaceList),
                Method::POST => Some(Self::NamespaceCreate),
                _ => None,
            };
        }
        let mut parts = path.strip_prefix("/v1/namespaces/")?.split('/');
        if parts.next()?.is_empty() {
            return None;
        }
        match (
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
            parts.next(),
        ) {
            (None, None, None, None, None) => match *method {
                Method::GET => Some(Self::NamespaceLoad),
                Method::HEAD => Some(Self::NamespaceExists),
                Method::DELETE => Some(Self::NamespaceDrop),
                _ => None,
            },
            (Some("properties"), None, None, None, None) if method == Method::POST => {
                Some(Self::NamespaceProperties)
            }
            (Some("tables"), None, None, None, None) => match *method {
                Method::GET => Some(Self::TableList),
                Method::POST => Some(Self::TableCreate),
                _ => None,
            },
            (Some("tables"), Some(table), None, None, None) if !table.is_empty() => match *method {
                Method::GET => Some(Self::TableLoad),
                Method::HEAD => Some(Self::TableExists),
                Method::POST => Some(Self::TableUpdate),
                Method::DELETE => Some(Self::TableDrop),
                _ => None,
            },
            (Some("tables"), Some(table), Some("credentials"), None, None)
                if !table.is_empty() && method == Method::GET =>
            {
                Some(Self::TableCredentials)
            }
            _ => None,
        }
    }

    pub(super) fn enabled(self, installed: &InstalledRoutes) -> bool {
        match self {
            Self::Config | Self::AdminMetrics => true,
            Self::NamespaceList
            | Self::NamespaceCreate
            | Self::NamespaceLoad
            | Self::NamespaceExists
            | Self::NamespaceProperties
            | Self::NamespaceDrop => installed.contains(InstalledRoutes::NAMESPACES),
            Self::TableList | Self::TableLoad | Self::TableExists => {
                installed.contains(InstalledRoutes::NAMESPACES | InstalledRoutes::TABLES)
            }
            Self::TableCreate | Self::TableUpdate | Self::TableDrop | Self::TableRename => {
                installed.contains(InstalledRoutes::NAMESPACES | InstalledRoutes::WRITES)
            }
            Self::TableCredentials => {
                installed.contains(InstalledRoutes::NAMESPACES | InstalledRoutes::CREDENTIALS)
            }
        }
    }

    pub(super) fn supported(self, capabilities: Capabilities) -> bool {
        let any = |action| (1..=3).any(|version| capabilities.supports(version, action));
        match self {
            Self::TableList | Self::TableLoad | Self::TableExists | Self::TableCredentials => {
                any(FormatAction::Read)
            }
            Self::TableCreate => any(FormatAction::Create),
            Self::TableUpdate => {
                any(FormatAction::Write) || capabilities.upgrade_v1_v2 || capabilities.upgrade_v2_v3
            }
            Self::TableDrop | Self::TableRename => any(FormatAction::Write),
            _ => true,
        }
    }

    pub(super) fn endpoints(installed: &InstalledRoutes, capabilities: Capabilities) -> Vec<String> {
        Self::ADVERTISED
            .iter()
            .filter(|route| route.enabled(installed) && route.supported(capabilities))
            .filter_map(|route| route.template())
            .map(str::to_owned)
            .collect()
    }

    fn template(self) -> Option<&'static str> {
        match self {
            Self::Config | Self::AdminMetrics => None,
            Self::NamespaceList => Some("GET /v1/{prefix}/namespaces"),
            Self::NamespaceCreate => Some("POST /v1/{prefix}/namespaces"),
            Self::NamespaceLoad => Some("GET /v1/{prefix}/namespaces/{namespace}"),
            Self::NamespaceExists => Some("HEAD /v1/{prefix}/namespaces/{namespace}"),
            Self::NamespaceProperties => Some("POST /v1/{prefix}/namespaces/{namespace}/properties"),
            Self::NamespaceDrop => Some("DELETE /v1/{prefix}/namespaces/{namespace}"),
            Self::TableList => Some("GET /v1/{prefix}/namespaces/{namespace}/tables"),
            Self::TableCreate => Some("POST /v1/{prefix}/namespaces/{namespace}/tables"),
            Self::TableLoad => Some("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}"),
            Self::TableExists => Some("HEAD /v1/{prefix}/namespaces/{namespace}/tables/{table}"),
            Self::TableUpdate => Some("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}"),
            Self::TableDrop => Some("DELETE /v1/{prefix}/namespaces/{namespace}/tables/{table}"),
            Self::TableRename => Some("POST /v1/{prefix}/tables/rename"),
            Self::TableCredentials => {
                Some("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/credentials")
            }
        }
    }
}
