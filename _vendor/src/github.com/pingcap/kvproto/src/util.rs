use eraftpb;
use pdpb;

impl From<pdpb::ConfChangeType> for eraftpb::ConfChangeType {
    fn from(ct: pdpb::ConfChangeType) -> Self {
        match ct {
            pdpb::ConfChangeType::AddNode => eraftpb::ConfChangeType::AddNode,
            pdpb::ConfChangeType::RemoveNode => eraftpb::ConfChangeType::RemoveNode,
        }
    }
}

impl From<eraftpb::ConfChangeType> for pdpb::ConfChangeType {
    fn from(ct: eraftpb::ConfChangeType) -> Self {
        match ct {
            eraftpb::ConfChangeType::AddNode => pdpb::ConfChangeType::AddNode,
            eraftpb::ConfChangeType::RemoveNode => pdpb::ConfChangeType::RemoveNode,
        }
    }
}
