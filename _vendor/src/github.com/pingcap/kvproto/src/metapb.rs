// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(PartialEq,Clone,Default)]
pub struct Cluster {
    // message fields
    id: ::std::option::Option<u64>,
    max_peer_count: ::std::option::Option<u32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Cluster {}

impl Cluster {
    pub fn new() -> Cluster {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Cluster {
        static mut instance: ::protobuf::lazy::Lazy<Cluster> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Cluster,
        };
        unsafe {
            instance.get(Cluster::new)
        }
    }

    // optional uint64 id = 1;

    pub fn clear_id(&mut self) {
        self.id = ::std::option::Option::None;
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: u64) {
        self.id = ::std::option::Option::Some(v);
    }

    pub fn get_id(&self) -> u64 {
        self.id.unwrap_or(0)
    }

    fn get_id_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.id
    }

    // optional uint32 max_peer_count = 2;

    pub fn clear_max_peer_count(&mut self) {
        self.max_peer_count = ::std::option::Option::None;
    }

    pub fn has_max_peer_count(&self) -> bool {
        self.max_peer_count.is_some()
    }

    // Param is passed by value, moved
    pub fn set_max_peer_count(&mut self, v: u32) {
        self.max_peer_count = ::std::option::Option::Some(v);
    }

    pub fn get_max_peer_count(&self) -> u32 {
        self.max_peer_count.unwrap_or(0)
    }

    fn get_max_peer_count_for_reflect(&self) -> &::std::option::Option<u32> {
        &self.max_peer_count
    }

    fn mut_max_peer_count_for_reflect(&mut self) -> &mut ::std::option::Option<u32> {
        &mut self.max_peer_count
    }
}

impl ::protobuf::Message for Cluster {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint32()?;
                    self.max_peer_count = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.max_peer_count {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.id {
            os.write_uint64(1, v)?;
        }
        if let Some(v) = self.max_peer_count {
            os.write_uint32(2, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Cluster {
    fn new() -> Cluster {
        Cluster::new()
    }

    fn descriptor_static(_: ::std::option::Option<Cluster>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "id",
                    Cluster::get_id_for_reflect,
                    Cluster::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint32>(
                    "max_peer_count",
                    Cluster::get_max_peer_count_for_reflect,
                    Cluster::mut_max_peer_count_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Cluster>(
                    "Cluster",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Cluster {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_max_peer_count();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Cluster {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Cluster {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct StoreLabel {
    // message fields
    key: ::protobuf::SingularField<::std::string::String>,
    value: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for StoreLabel {}

impl StoreLabel {
    pub fn new() -> StoreLabel {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static StoreLabel {
        static mut instance: ::protobuf::lazy::Lazy<StoreLabel> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const StoreLabel,
        };
        unsafe {
            instance.get(StoreLabel::new)
        }
    }

    // optional string key = 1;

    pub fn clear_key(&mut self) {
        self.key.clear();
    }

    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_key(&mut self, v: ::std::string::String) {
        self.key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key(&mut self) -> &mut ::std::string::String {
        if self.key.is_none() {
            self.key.set_default();
        }
        self.key.as_mut().unwrap()
    }

    // Take field
    pub fn take_key(&mut self) -> ::std::string::String {
        self.key.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_key(&self) -> &str {
        match self.key.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_key_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.key
    }

    fn mut_key_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.key
    }

    // optional string value = 2;

    pub fn clear_value(&mut self) {
        self.value.clear();
    }

    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    // Param is passed by value, moved
    pub fn set_value(&mut self, v: ::std::string::String) {
        self.value = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_value(&mut self) -> &mut ::std::string::String {
        if self.value.is_none() {
            self.value.set_default();
        }
        self.value.as_mut().unwrap()
    }

    // Take field
    pub fn take_value(&mut self) -> ::std::string::String {
        self.value.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_value(&self) -> &str {
        match self.value.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_value_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.value
    }

    fn mut_value_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.value
    }
}

impl ::protobuf::Message for StoreLabel {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.key)?;
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.value)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(ref v) = self.key.as_ref() {
            my_size += ::protobuf::rt::string_size(1, &v);
        }
        if let Some(ref v) = self.value.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(ref v) = self.key.as_ref() {
            os.write_string(1, &v)?;
        }
        if let Some(ref v) = self.value.as_ref() {
            os.write_string(2, &v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for StoreLabel {
    fn new() -> StoreLabel {
        StoreLabel::new()
    }

    fn descriptor_static(_: ::std::option::Option<StoreLabel>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "key",
                    StoreLabel::get_key_for_reflect,
                    StoreLabel::mut_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "value",
                    StoreLabel::get_value_for_reflect,
                    StoreLabel::mut_value_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<StoreLabel>(
                    "StoreLabel",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for StoreLabel {
    fn clear(&mut self) {
        self.clear_key();
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for StoreLabel {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for StoreLabel {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Store {
    // message fields
    id: ::std::option::Option<u64>,
    address: ::protobuf::SingularField<::std::string::String>,
    state: ::std::option::Option<StoreState>,
    labels: ::protobuf::RepeatedField<StoreLabel>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Store {}

impl Store {
    pub fn new() -> Store {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Store {
        static mut instance: ::protobuf::lazy::Lazy<Store> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Store,
        };
        unsafe {
            instance.get(Store::new)
        }
    }

    // optional uint64 id = 1;

    pub fn clear_id(&mut self) {
        self.id = ::std::option::Option::None;
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: u64) {
        self.id = ::std::option::Option::Some(v);
    }

    pub fn get_id(&self) -> u64 {
        self.id.unwrap_or(0)
    }

    fn get_id_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.id
    }

    // optional string address = 2;

    pub fn clear_address(&mut self) {
        self.address.clear();
    }

    pub fn has_address(&self) -> bool {
        self.address.is_some()
    }

    // Param is passed by value, moved
    pub fn set_address(&mut self, v: ::std::string::String) {
        self.address = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_address(&mut self) -> &mut ::std::string::String {
        if self.address.is_none() {
            self.address.set_default();
        }
        self.address.as_mut().unwrap()
    }

    // Take field
    pub fn take_address(&mut self) -> ::std::string::String {
        self.address.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_address(&self) -> &str {
        match self.address.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    fn get_address_for_reflect(&self) -> &::protobuf::SingularField<::std::string::String> {
        &self.address
    }

    fn mut_address_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::string::String> {
        &mut self.address
    }

    // optional .metapb.StoreState state = 3;

    pub fn clear_state(&mut self) {
        self.state = ::std::option::Option::None;
    }

    pub fn has_state(&self) -> bool {
        self.state.is_some()
    }

    // Param is passed by value, moved
    pub fn set_state(&mut self, v: StoreState) {
        self.state = ::std::option::Option::Some(v);
    }

    pub fn get_state(&self) -> StoreState {
        self.state.unwrap_or(StoreState::Up)
    }

    fn get_state_for_reflect(&self) -> &::std::option::Option<StoreState> {
        &self.state
    }

    fn mut_state_for_reflect(&mut self) -> &mut ::std::option::Option<StoreState> {
        &mut self.state
    }

    // repeated .metapb.StoreLabel labels = 4;

    pub fn clear_labels(&mut self) {
        self.labels.clear();
    }

    // Param is passed by value, moved
    pub fn set_labels(&mut self, v: ::protobuf::RepeatedField<StoreLabel>) {
        self.labels = v;
    }

    // Mutable pointer to the field.
    pub fn mut_labels(&mut self) -> &mut ::protobuf::RepeatedField<StoreLabel> {
        &mut self.labels
    }

    // Take field
    pub fn take_labels(&mut self) -> ::protobuf::RepeatedField<StoreLabel> {
        ::std::mem::replace(&mut self.labels, ::protobuf::RepeatedField::new())
    }

    pub fn get_labels(&self) -> &[StoreLabel] {
        &self.labels
    }

    fn get_labels_for_reflect(&self) -> &::protobuf::RepeatedField<StoreLabel> {
        &self.labels
    }

    fn mut_labels_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<StoreLabel> {
        &mut self.labels
    }
}

impl ::protobuf::Message for Store {
    fn is_initialized(&self) -> bool {
        for v in &self.labels {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.address)?;
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_enum()?;
                    self.state = ::std::option::Option::Some(tmp);
                },
                4 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.labels)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.address.as_ref() {
            my_size += ::protobuf::rt::string_size(2, &v);
        }
        if let Some(v) = self.state {
            my_size += ::protobuf::rt::enum_size(3, v);
        }
        for value in &self.labels {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.id {
            os.write_uint64(1, v)?;
        }
        if let Some(ref v) = self.address.as_ref() {
            os.write_string(2, &v)?;
        }
        if let Some(v) = self.state {
            os.write_enum(3, v.value())?;
        }
        for v in &self.labels {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Store {
    fn new() -> Store {
        Store::new()
    }

    fn descriptor_static(_: ::std::option::Option<Store>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "id",
                    Store::get_id_for_reflect,
                    Store::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeString>(
                    "address",
                    Store::get_address_for_reflect,
                    Store::mut_address_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeEnum<StoreState>>(
                    "state",
                    Store::get_state_for_reflect,
                    Store::mut_state_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<StoreLabel>>(
                    "labels",
                    Store::get_labels_for_reflect,
                    Store::mut_labels_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Store>(
                    "Store",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Store {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_address();
        self.clear_state();
        self.clear_labels();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Store {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Store {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct RegionEpoch {
    // message fields
    conf_ver: ::std::option::Option<u64>,
    version: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RegionEpoch {}

impl RegionEpoch {
    pub fn new() -> RegionEpoch {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RegionEpoch {
        static mut instance: ::protobuf::lazy::Lazy<RegionEpoch> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RegionEpoch,
        };
        unsafe {
            instance.get(RegionEpoch::new)
        }
    }

    // optional uint64 conf_ver = 1;

    pub fn clear_conf_ver(&mut self) {
        self.conf_ver = ::std::option::Option::None;
    }

    pub fn has_conf_ver(&self) -> bool {
        self.conf_ver.is_some()
    }

    // Param is passed by value, moved
    pub fn set_conf_ver(&mut self, v: u64) {
        self.conf_ver = ::std::option::Option::Some(v);
    }

    pub fn get_conf_ver(&self) -> u64 {
        self.conf_ver.unwrap_or(0)
    }

    fn get_conf_ver_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.conf_ver
    }

    fn mut_conf_ver_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.conf_ver
    }

    // optional uint64 version = 2;

    pub fn clear_version(&mut self) {
        self.version = ::std::option::Option::None;
    }

    pub fn has_version(&self) -> bool {
        self.version.is_some()
    }

    // Param is passed by value, moved
    pub fn set_version(&mut self, v: u64) {
        self.version = ::std::option::Option::Some(v);
    }

    pub fn get_version(&self) -> u64 {
        self.version.unwrap_or(0)
    }

    fn get_version_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.version
    }

    fn mut_version_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.version
    }
}

impl ::protobuf::Message for RegionEpoch {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.conf_ver = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.version = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.conf_ver {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.version {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.conf_ver {
            os.write_uint64(1, v)?;
        }
        if let Some(v) = self.version {
            os.write_uint64(2, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RegionEpoch {
    fn new() -> RegionEpoch {
        RegionEpoch::new()
    }

    fn descriptor_static(_: ::std::option::Option<RegionEpoch>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "conf_ver",
                    RegionEpoch::get_conf_ver_for_reflect,
                    RegionEpoch::mut_conf_ver_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "version",
                    RegionEpoch::get_version_for_reflect,
                    RegionEpoch::mut_version_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RegionEpoch>(
                    "RegionEpoch",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RegionEpoch {
    fn clear(&mut self) {
        self.clear_conf_ver();
        self.clear_version();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for RegionEpoch {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for RegionEpoch {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Region {
    // message fields
    id: ::std::option::Option<u64>,
    start_key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    end_key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    region_epoch: ::protobuf::SingularPtrField<RegionEpoch>,
    peers: ::protobuf::RepeatedField<Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Region {}

impl Region {
    pub fn new() -> Region {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Region {
        static mut instance: ::protobuf::lazy::Lazy<Region> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Region,
        };
        unsafe {
            instance.get(Region::new)
        }
    }

    // optional uint64 id = 1;

    pub fn clear_id(&mut self) {
        self.id = ::std::option::Option::None;
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: u64) {
        self.id = ::std::option::Option::Some(v);
    }

    pub fn get_id(&self) -> u64 {
        self.id.unwrap_or(0)
    }

    fn get_id_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.id
    }

    // optional bytes start_key = 2;

    pub fn clear_start_key(&mut self) {
        self.start_key.clear();
    }

    pub fn has_start_key(&self) -> bool {
        self.start_key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_start_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.start_key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_start_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.start_key.is_none() {
            self.start_key.set_default();
        }
        self.start_key.as_mut().unwrap()
    }

    // Take field
    pub fn take_start_key(&mut self) -> ::std::vec::Vec<u8> {
        self.start_key.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_start_key(&self) -> &[u8] {
        match self.start_key.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_start_key_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.start_key
    }

    fn mut_start_key_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.start_key
    }

    // optional bytes end_key = 3;

    pub fn clear_end_key(&mut self) {
        self.end_key.clear();
    }

    pub fn has_end_key(&self) -> bool {
        self.end_key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_end_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.end_key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_end_key(&mut self) -> &mut ::std::vec::Vec<u8> {
        if self.end_key.is_none() {
            self.end_key.set_default();
        }
        self.end_key.as_mut().unwrap()
    }

    // Take field
    pub fn take_end_key(&mut self) -> ::std::vec::Vec<u8> {
        self.end_key.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_end_key(&self) -> &[u8] {
        match self.end_key.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    fn get_end_key_for_reflect(&self) -> &::protobuf::SingularField<::std::vec::Vec<u8>> {
        &self.end_key
    }

    fn mut_end_key_for_reflect(&mut self) -> &mut ::protobuf::SingularField<::std::vec::Vec<u8>> {
        &mut self.end_key
    }

    // optional .metapb.RegionEpoch region_epoch = 4;

    pub fn clear_region_epoch(&mut self) {
        self.region_epoch.clear();
    }

    pub fn has_region_epoch(&self) -> bool {
        self.region_epoch.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_epoch(&mut self, v: RegionEpoch) {
        self.region_epoch = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_epoch(&mut self) -> &mut RegionEpoch {
        if self.region_epoch.is_none() {
            self.region_epoch.set_default();
        }
        self.region_epoch.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_epoch(&mut self) -> RegionEpoch {
        self.region_epoch.take().unwrap_or_else(|| RegionEpoch::new())
    }

    pub fn get_region_epoch(&self) -> &RegionEpoch {
        self.region_epoch.as_ref().unwrap_or_else(|| RegionEpoch::default_instance())
    }

    fn get_region_epoch_for_reflect(&self) -> &::protobuf::SingularPtrField<RegionEpoch> {
        &self.region_epoch
    }

    fn mut_region_epoch_for_reflect(&mut self) -> &mut ::protobuf::SingularPtrField<RegionEpoch> {
        &mut self.region_epoch
    }

    // repeated .metapb.Peer peers = 5;

    pub fn clear_peers(&mut self) {
        self.peers.clear();
    }

    // Param is passed by value, moved
    pub fn set_peers(&mut self, v: ::protobuf::RepeatedField<Peer>) {
        self.peers = v;
    }

    // Mutable pointer to the field.
    pub fn mut_peers(&mut self) -> &mut ::protobuf::RepeatedField<Peer> {
        &mut self.peers
    }

    // Take field
    pub fn take_peers(&mut self) -> ::protobuf::RepeatedField<Peer> {
        ::std::mem::replace(&mut self.peers, ::protobuf::RepeatedField::new())
    }

    pub fn get_peers(&self) -> &[Peer] {
        &self.peers
    }

    fn get_peers_for_reflect(&self) -> &::protobuf::RepeatedField<Peer> {
        &self.peers
    }

    fn mut_peers_for_reflect(&mut self) -> &mut ::protobuf::RepeatedField<Peer> {
        &mut self.peers
    }
}

impl ::protobuf::Message for Region {
    fn is_initialized(&self) -> bool {
        for v in &self.region_epoch {
            if !v.is_initialized() {
                return false;
            }
        };
        for v in &self.peers {
            if !v.is_initialized() {
                return false;
            }
        };
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.start_key)?;
                },
                3 => {
                    ::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.end_key)?;
                },
                4 => {
                    ::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region_epoch)?;
                },
                5 => {
                    ::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.peers)?;
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(ref v) = self.start_key.as_ref() {
            my_size += ::protobuf::rt::bytes_size(2, &v);
        }
        if let Some(ref v) = self.end_key.as_ref() {
            my_size += ::protobuf::rt::bytes_size(3, &v);
        }
        if let Some(ref v) = self.region_epoch.as_ref() {
            let len = v.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        for value in &self.peers {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.id {
            os.write_uint64(1, v)?;
        }
        if let Some(ref v) = self.start_key.as_ref() {
            os.write_bytes(2, &v)?;
        }
        if let Some(ref v) = self.end_key.as_ref() {
            os.write_bytes(3, &v)?;
        }
        if let Some(ref v) = self.region_epoch.as_ref() {
            os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        }
        for v in &self.peers {
            os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited)?;
            os.write_raw_varint32(v.get_cached_size())?;
            v.write_to_with_cached_sizes(os)?;
        };
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Region {
    fn new() -> Region {
        Region::new()
    }

    fn descriptor_static(_: ::std::option::Option<Region>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "id",
                    Region::get_id_for_reflect,
                    Region::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "start_key",
                    Region::get_start_key_for_reflect,
                    Region::mut_start_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_field_accessor::<_, ::protobuf::types::ProtobufTypeBytes>(
                    "end_key",
                    Region::get_end_key_for_reflect,
                    Region::mut_end_key_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_ptr_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<RegionEpoch>>(
                    "region_epoch",
                    Region::get_region_epoch_for_reflect,
                    Region::mut_region_epoch_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_field_accessor::<_, ::protobuf::types::ProtobufTypeMessage<Peer>>(
                    "peers",
                    Region::get_peers_for_reflect,
                    Region::mut_peers_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Region>(
                    "Region",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Region {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_start_key();
        self.clear_end_key();
        self.clear_region_epoch();
        self.clear_peers();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Region {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Region {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(PartialEq,Clone,Default)]
pub struct Peer {
    // message fields
    id: ::std::option::Option<u64>,
    store_id: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::protobuf::CachedSize,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Peer {}

impl Peer {
    pub fn new() -> Peer {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Peer {
        static mut instance: ::protobuf::lazy::Lazy<Peer> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Peer,
        };
        unsafe {
            instance.get(Peer::new)
        }
    }

    // optional uint64 id = 1;

    pub fn clear_id(&mut self) {
        self.id = ::std::option::Option::None;
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: u64) {
        self.id = ::std::option::Option::Some(v);
    }

    pub fn get_id(&self) -> u64 {
        self.id.unwrap_or(0)
    }

    fn get_id_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.id
    }

    fn mut_id_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.id
    }

    // optional uint64 store_id = 2;

    pub fn clear_store_id(&mut self) {
        self.store_id = ::std::option::Option::None;
    }

    pub fn has_store_id(&self) -> bool {
        self.store_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store_id(&mut self, v: u64) {
        self.store_id = ::std::option::Option::Some(v);
    }

    pub fn get_store_id(&self) -> u64 {
        self.store_id.unwrap_or(0)
    }

    fn get_store_id_for_reflect(&self) -> &::std::option::Option<u64> {
        &self.store_id
    }

    fn mut_store_id_for_reflect(&mut self) -> &mut ::std::option::Option<u64> {
        &mut self.store_id
    }
}

impl ::protobuf::Message for Peer {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !is.eof()? {
            let (field_number, wire_type) = is.read_tag_unpack()?;
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    }
                    let tmp = is.read_uint64()?;
                    self.store_id = ::std::option::Option::Some(tmp);
                },
                _ => {
                    ::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields())?;
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if let Some(v) = self.id {
            my_size += ::protobuf::rt::value_size(1, v, ::protobuf::wire_format::WireTypeVarint);
        }
        if let Some(v) = self.store_id {
            my_size += ::protobuf::rt::value_size(2, v, ::protobuf::wire_format::WireTypeVarint);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.id {
            os.write_uint64(1, v)?;
        }
        if let Some(v) = self.store_id {
            os.write_uint64(2, v)?;
        }
        os.write_unknown_fields(self.get_unknown_fields())?;
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }
    fn as_any_mut(&mut self) -> &mut ::std::any::Any {
        self as &mut ::std::any::Any
    }
    fn into_any(self: Box<Self>) -> ::std::boxed::Box<::std::any::Any> {
        self
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Peer {
    fn new() -> Peer {
        Peer::new()
    }

    fn descriptor_static(_: ::std::option::Option<Peer>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "id",
                    Peer::get_id_for_reflect,
                    Peer::mut_id_for_reflect,
                ));
                fields.push(::protobuf::reflect::accessor::make_option_accessor::<_, ::protobuf::types::ProtobufTypeUint64>(
                    "store_id",
                    Peer::get_store_id_for_reflect,
                    Peer::mut_store_id_for_reflect,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Peer>(
                    "Peer",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Peer {
    fn clear(&mut self) {
        self.clear_id();
        self.clear_store_id();
        self.unknown_fields.clear();
    }
}

impl ::std::fmt::Debug for Peer {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

impl ::protobuf::reflect::ProtobufValue for Peer {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Message(self)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum StoreState {
    Up = 0,
    Offline = 1,
    Tombstone = 2,
}

impl ::protobuf::ProtobufEnum for StoreState {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<StoreState> {
        match value {
            0 => ::std::option::Option::Some(StoreState::Up),
            1 => ::std::option::Option::Some(StoreState::Offline),
            2 => ::std::option::Option::Some(StoreState::Tombstone),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [StoreState] = &[
            StoreState::Up,
            StoreState::Offline,
            StoreState::Tombstone,
        ];
        values
    }

    fn enum_descriptor_static(_: ::std::option::Option<StoreState>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("StoreState", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for StoreState {
}

impl ::protobuf::reflect::ProtobufValue for StoreState {
    fn as_ref(&self) -> ::protobuf::reflect::ProtobufValueRef {
        ::protobuf::reflect::ProtobufValueRef::Enum(self.descriptor())
    }
}

static file_descriptor_proto_data: &'static [u8] = b"\
    \n\x0cmetapb.proto\x12\x06metapb\x1a\x14gogoproto/gogo.proto\"K\n\x07Clu\
    ster\x12\x14\n\x02id\x18\x01\x20\x01(\x04R\x02idB\x04\xc8\xde\x1f\0\x12*\
    \n\x0emax_peer_count\x18\x02\x20\x01(\rR\x0cmaxPeerCountB\x04\xc8\xde\
    \x1f\0\"@\n\nStoreLabel\x12\x16\n\x03key\x18\x01\x20\x01(\tR\x03keyB\x04\
    \xc8\xde\x1f\0\x12\x1a\n\x05value\x18\x02\x20\x01(\tR\x05valueB\x04\xc8\
    \xde\x1f\0\"\x99\x01\n\x05Store\x12\x14\n\x02id\x18\x01\x20\x01(\x04R\
    \x02idB\x04\xc8\xde\x1f\0\x12\x1e\n\x07address\x18\x02\x20\x01(\tR\x07ad\
    dressB\x04\xc8\xde\x1f\0\x12.\n\x05state\x18\x03\x20\x01(\x0e2\x12.metap\
    b.StoreStateR\x05stateB\x04\xc8\xde\x1f\0\x12*\n\x06labels\x18\x04\x20\
    \x03(\x0b2\x12.metapb.StoreLabelR\x06labels\"N\n\x0bRegionEpoch\x12\x1f\
    \n\x08conf_ver\x18\x01\x20\x01(\x04R\x07confVerB\x04\xc8\xde\x1f\0\x12\
    \x1e\n\x07version\x18\x02\x20\x01(\x04R\x07versionB\x04\xc8\xde\x1f\0\"\
    \xb0\x01\n\x06Region\x12\x14\n\x02id\x18\x01\x20\x01(\x04R\x02idB\x04\
    \xc8\xde\x1f\0\x12\x1b\n\tstart_key\x18\x02\x20\x01(\x0cR\x08startKey\
    \x12\x17\n\x07end_key\x18\x03\x20\x01(\x0cR\x06endKey\x126\n\x0cregion_e\
    poch\x18\x04\x20\x01(\x0b2\x13.metapb.RegionEpochR\x0bregionEpoch\x12\"\
    \n\x05peers\x18\x05\x20\x03(\x0b2\x0c.metapb.PeerR\x05peers\"=\n\x04Peer\
    \x12\x14\n\x02id\x18\x01\x20\x01(\x04R\x02idB\x04\xc8\xde\x1f\0\x12\x1f\
    \n\x08store_id\x18\x02\x20\x01(\x04R\x07storeIdB\x04\xc8\xde\x1f\0*0\n\n\
    StoreState\x12\x06\n\x02Up\x10\0\x12\x0b\n\x07Offline\x10\x01\x12\r\n\tT\
    ombstone\x10\x02B&\n\x18com.pingcap.tikv.kvproto\xe0\xe2\x1e\x01\xc8\xe2\
    \x1e\x01\xd0\xe2\x1e\x01J\x86\x1b\n\x06\x12\x04\0\0:\x01\n\x08\n\x01\x0c\
    \x12\x03\0\0\x12\n\x08\n\x01\x02\x12\x03\x01\x08\x0e\n\t\n\x02\x03\0\x12\
    \x03\x03\x07\x1d\n\x08\n\x01\x08\x12\x03\x05\0(\n\x0b\n\x04\x08\xe7\x07\
    \0\x12\x03\x05\0(\n\x0c\n\x05\x08\xe7\x07\0\x02\x12\x03\x05\x07\x20\n\r\
    \n\x06\x08\xe7\x07\0\x02\0\x12\x03\x05\x07\x20\n\x0e\n\x07\x08\xe7\x07\0\
    \x02\0\x01\x12\x03\x05\x08\x1f\n\x0c\n\x05\x08\xe7\x07\0\x03\x12\x03\x05\
    #'\n\x08\n\x01\x08\x12\x03\x06\0$\n\x0b\n\x04\x08\xe7\x07\x01\x12\x03\
    \x06\0$\n\x0c\n\x05\x08\xe7\x07\x01\x02\x12\x03\x06\x07\x1c\n\r\n\x06\
    \x08\xe7\x07\x01\x02\0\x12\x03\x06\x07\x1c\n\x0e\n\x07\x08\xe7\x07\x01\
    \x02\0\x01\x12\x03\x06\x08\x1b\n\x0c\n\x05\x08\xe7\x07\x01\x03\x12\x03\
    \x06\x1f#\n\x08\n\x01\x08\x12\x03\x07\0*\n\x0b\n\x04\x08\xe7\x07\x02\x12\
    \x03\x07\0*\n\x0c\n\x05\x08\xe7\x07\x02\x02\x12\x03\x07\x07\"\n\r\n\x06\
    \x08\xe7\x07\x02\x02\0\x12\x03\x07\x07\"\n\x0e\n\x07\x08\xe7\x07\x02\x02\
    \0\x01\x12\x03\x07\x08!\n\x0c\n\x05\x08\xe7\x07\x02\x03\x12\x03\x07%)\n\
    \x08\n\x01\x08\x12\x03\t\01\n\x0b\n\x04\x08\xe7\x07\x03\x12\x03\t\01\n\
    \x0c\n\x05\x08\xe7\x07\x03\x02\x12\x03\t\x07\x13\n\r\n\x06\x08\xe7\x07\
    \x03\x02\0\x12\x03\t\x07\x13\n\x0e\n\x07\x08\xe7\x07\x03\x02\0\x01\x12\
    \x03\t\x07\x13\n\x0c\n\x05\x08\xe7\x07\x03\x07\x12\x03\t\x160\n\n\n\x02\
    \x04\0\x12\x04\x0b\0\x11\x01\n\n\n\x03\x04\0\x01\x12\x03\x0b\x08\x0f\n\
    \x0b\n\x04\x04\0\x02\0\x12\x03\x0c\x04G\n\x0c\n\x05\x04\0\x02\0\x04\x12\
    \x03\x0c\x04\x0c\n\x0c\n\x05\x04\0\x02\0\x05\x12\x03\x0c\r\x13\n\x0c\n\
    \x05\x04\0\x02\0\x01\x12\x03\x0c\x14\x16\n\x0c\n\x05\x04\0\x02\0\x03\x12\
    \x03\x0c&'\n\x0c\n\x05\x04\0\x02\0\x08\x12\x03\x0c(F\n\x0f\n\x08\x04\0\
    \x02\0\x08\xe7\x07\0\x12\x03\x0c)E\n\x10\n\t\x04\0\x02\0\x08\xe7\x07\0\
    \x02\x12\x03\x0c)=\n\x11\n\n\x04\0\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x0c\
    )=\n\x12\n\x0b\x04\0\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03\x0c*<\n\x10\n\
    \t\x04\0\x02\0\x08\xe7\x07\0\x03\x12\x03\x0c@E\n\x82\x01\n\x04\x04\0\x02\
    \x01\x12\x03\x0f\x04G\x1a\\\x20max\x20peer\x20count\x20for\x20a\x20regio\
    n.\n\x20pd\x20will\x20do\x20the\x20auto-balance\x20if\x20region\x20peer\
    \x20count\x20mismatches.\n\"\x17\x20more\x20attributes......\n\n\x0c\n\
    \x05\x04\0\x02\x01\x04\x12\x03\x0f\x04\x0c\n\x0c\n\x05\x04\0\x02\x01\x05\
    \x12\x03\x0f\r\x13\n\x0c\n\x05\x04\0\x02\x01\x01\x12\x03\x0f\x14\"\n\x0c\
    \n\x05\x04\0\x02\x01\x03\x12\x03\x0f&'\n\x0c\n\x05\x04\0\x02\x01\x08\x12\
    \x03\x0f(F\n\x0f\n\x08\x04\0\x02\x01\x08\xe7\x07\0\x12\x03\x0f)E\n\x10\n\
    \t\x04\0\x02\x01\x08\xe7\x07\0\x02\x12\x03\x0f)=\n\x11\n\n\x04\0\x02\x01\
    \x08\xe7\x07\0\x02\0\x12\x03\x0f)=\n\x12\n\x0b\x04\0\x02\x01\x08\xe7\x07\
    \0\x02\0\x01\x12\x03\x0f*<\n\x10\n\t\x04\0\x02\x01\x08\xe7\x07\0\x03\x12\
    \x03\x0f@E\n\n\n\x02\x05\0\x12\x04\x13\0\x17\x01\n\n\n\x03\x05\0\x01\x12\
    \x03\x13\x05\x0f\n\x0b\n\x04\x05\0\x02\0\x12\x03\x14\x04\x12\n\x0c\n\x05\
    \x05\0\x02\0\x01\x12\x03\x14\x04\x06\n\x0c\n\x05\x05\0\x02\0\x02\x12\x03\
    \x14\x10\x11\n\x0b\n\x04\x05\0\x02\x01\x12\x03\x15\x04\x12\n\x0c\n\x05\
    \x05\0\x02\x01\x01\x12\x03\x15\x04\x0b\n\x0c\n\x05\x05\0\x02\x01\x02\x12\
    \x03\x15\x10\x11\n\x0b\n\x04\x05\0\x02\x02\x12\x03\x16\x04\x12\n\x0c\n\
    \x05\x05\0\x02\x02\x01\x12\x03\x16\x04\r\n\x0c\n\x05\x05\0\x02\x02\x02\
    \x12\x03\x16\x10\x11\nA\n\x02\x04\x01\x12\x04\x1a\0\x1d\x01\x1a5\x20Case\
    \x20insensitive\x20key/value\x20for\x20replica\x20constraints.\n\n\n\n\
    \x03\x04\x01\x01\x12\x03\x1a\x08\x12\n\x0b\n\x04\x04\x01\x02\0\x12\x03\
    \x1b\x04C\n\x0c\n\x05\x04\x01\x02\0\x04\x12\x03\x1b\x04\x0c\n\x0c\n\x05\
    \x04\x01\x02\0\x05\x12\x03\x1b\r\x13\n\x0c\n\x05\x04\x01\x02\0\x01\x12\
    \x03\x1b\x14\x17\n\x0c\n\x05\x04\x01\x02\0\x03\x12\x03\x1b\"#\n\x0c\n\
    \x05\x04\x01\x02\0\x08\x12\x03\x1b$B\n\x0f\n\x08\x04\x01\x02\0\x08\xe7\
    \x07\0\x12\x03\x1b%A\n\x10\n\t\x04\x01\x02\0\x08\xe7\x07\0\x02\x12\x03\
    \x1b%9\n\x11\n\n\x04\x01\x02\0\x08\xe7\x07\0\x02\0\x12\x03\x1b%9\n\x12\n\
    \x0b\x04\x01\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03\x1b&8\n\x10\n\t\x04\
    \x01\x02\0\x08\xe7\x07\0\x03\x12\x03\x1b<A\n\x0b\n\x04\x04\x01\x02\x01\
    \x12\x03\x1c\x04C\n\x0c\n\x05\x04\x01\x02\x01\x04\x12\x03\x1c\x04\x0c\n\
    \x0c\n\x05\x04\x01\x02\x01\x05\x12\x03\x1c\r\x13\n\x0c\n\x05\x04\x01\x02\
    \x01\x01\x12\x03\x1c\x14\x19\n\x0c\n\x05\x04\x01\x02\x01\x03\x12\x03\x1c\
    \"#\n\x0c\n\x05\x04\x01\x02\x01\x08\x12\x03\x1c$B\n\x0f\n\x08\x04\x01\
    \x02\x01\x08\xe7\x07\0\x12\x03\x1c%A\n\x10\n\t\x04\x01\x02\x01\x08\xe7\
    \x07\0\x02\x12\x03\x1c%9\n\x11\n\n\x04\x01\x02\x01\x08\xe7\x07\0\x02\0\
    \x12\x03\x1c%9\n\x12\n\x0b\x04\x01\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\
    \x03\x1c&8\n\x10\n\t\x04\x01\x02\x01\x08\xe7\x07\0\x03\x12\x03\x1c<A\n\n\
    \n\x02\x04\x02\x12\x04\x1f\0%\x01\n\n\n\x03\x04\x02\x01\x12\x03\x1f\x08\
    \r\n\x0b\n\x04\x04\x02\x02\0\x12\x03\x20\x04C\n\x0c\n\x05\x04\x02\x02\0\
    \x04\x12\x03\x20\x04\x0c\n\x0c\n\x05\x04\x02\x02\0\x05\x12\x03\x20\r\x13\
    \n\x0c\n\x05\x04\x02\x02\0\x01\x12\x03\x20\x14\x16\n\x0c\n\x05\x04\x02\
    \x02\0\x03\x12\x03\x20\"#\n\x0c\n\x05\x04\x02\x02\0\x08\x12\x03\x20$B\n\
    \x0f\n\x08\x04\x02\x02\0\x08\xe7\x07\0\x12\x03\x20%A\n\x10\n\t\x04\x02\
    \x02\0\x08\xe7\x07\0\x02\x12\x03\x20%9\n\x11\n\n\x04\x02\x02\0\x08\xe7\
    \x07\0\x02\0\x12\x03\x20%9\n\x12\n\x0b\x04\x02\x02\0\x08\xe7\x07\0\x02\0\
    \x01\x12\x03\x20&8\n\x10\n\t\x04\x02\x02\0\x08\xe7\x07\0\x03\x12\x03\x20\
    <A\n\x0b\n\x04\x04\x02\x02\x01\x12\x03!\x04C\n\x0c\n\x05\x04\x02\x02\x01\
    \x04\x12\x03!\x04\x0c\n\x0c\n\x05\x04\x02\x02\x01\x05\x12\x03!\r\x13\n\
    \x0c\n\x05\x04\x02\x02\x01\x01\x12\x03!\x14\x1b\n\x0c\n\x05\x04\x02\x02\
    \x01\x03\x12\x03!\"#\n\x0c\n\x05\x04\x02\x02\x01\x08\x12\x03!$B\n\x0f\n\
    \x08\x04\x02\x02\x01\x08\xe7\x07\0\x12\x03!%A\n\x10\n\t\x04\x02\x02\x01\
    \x08\xe7\x07\0\x02\x12\x03!%9\n\x11\n\n\x04\x02\x02\x01\x08\xe7\x07\0\
    \x02\0\x12\x03!%9\n\x12\n\x0b\x04\x02\x02\x01\x08\xe7\x07\0\x02\0\x01\
    \x12\x03!&8\n\x10\n\t\x04\x02\x02\x01\x08\xe7\x07\0\x03\x12\x03!<A\n\x0b\
    \n\x04\x04\x02\x02\x02\x12\x03\"\x04C\n\x0c\n\x05\x04\x02\x02\x02\x04\
    \x12\x03\"\x04\x0c\n\x0c\n\x05\x04\x02\x02\x02\x06\x12\x03\"\r\x17\n\x0c\
    \n\x05\x04\x02\x02\x02\x01\x12\x03\"\x18\x1d\n\x0c\n\x05\x04\x02\x02\x02\
    \x03\x12\x03\"\"#\n\x0c\n\x05\x04\x02\x02\x02\x08\x12\x03\"$B\n\x0f\n\
    \x08\x04\x02\x02\x02\x08\xe7\x07\0\x12\x03\"%A\n\x10\n\t\x04\x02\x02\x02\
    \x08\xe7\x07\0\x02\x12\x03\"%9\n\x11\n\n\x04\x02\x02\x02\x08\xe7\x07\0\
    \x02\0\x12\x03\"%9\n\x12\n\x0b\x04\x02\x02\x02\x08\xe7\x07\0\x02\0\x01\
    \x12\x03\"&8\n\x10\n\t\x04\x02\x02\x02\x08\xe7\x07\0\x03\x12\x03\"<A\n$\
    \n\x04\x04\x02\x02\x03\x12\x03#\x04$\"\x17\x20more\x20attributes......\n\
    \n\x0c\n\x05\x04\x02\x02\x03\x04\x12\x03#\x04\x0c\n\x0c\n\x05\x04\x02\
    \x02\x03\x06\x12\x03#\r\x17\n\x0c\n\x05\x04\x02\x02\x03\x01\x12\x03#\x18\
    \x1e\n\x0c\n\x05\x04\x02\x02\x03\x03\x12\x03#\"#\n\n\n\x02\x04\x03\x12\
    \x04'\0,\x01\n\n\n\x03\x04\x03\x01\x12\x03'\x08\x13\nJ\n\x04\x04\x03\x02\
    \0\x12\x03)\x04C\x1a=\x20Conf\x20change\x20version,\x20auto\x20increment\
    \x20when\x20add\x20or\x20remove\x20peer\n\n\x0c\n\x05\x04\x03\x02\0\x04\
    \x12\x03)\x04\x0c\n\x0c\n\x05\x04\x03\x02\0\x05\x12\x03)\r\x13\n\x0c\n\
    \x05\x04\x03\x02\0\x01\x12\x03)\x14\x1c\n\x0c\n\x05\x04\x03\x02\0\x03\
    \x12\x03)\"#\n\x0c\n\x05\x04\x03\x02\0\x08\x12\x03)$B\n\x0f\n\x08\x04\
    \x03\x02\0\x08\xe7\x07\0\x12\x03)%A\n\x10\n\t\x04\x03\x02\0\x08\xe7\x07\
    \0\x02\x12\x03)%9\n\x11\n\n\x04\x03\x02\0\x08\xe7\x07\0\x02\0\x12\x03)%9\
    \n\x12\n\x0b\x04\x03\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x03)&8\n\x10\n\t\
    \x04\x03\x02\0\x08\xe7\x07\0\x03\x12\x03)<A\nA\n\x04\x04\x03\x02\x01\x12\
    \x03+\x04C\x1a4\x20Region\x20version,\x20auto\x20increment\x20when\x20sp\
    lit\x20or\x20merge\n\n\x0c\n\x05\x04\x03\x02\x01\x04\x12\x03+\x04\x0c\n\
    \x0c\n\x05\x04\x03\x02\x01\x05\x12\x03+\r\x13\n\x0c\n\x05\x04\x03\x02\
    \x01\x01\x12\x03+\x14\x1b\n\x0c\n\x05\x04\x03\x02\x01\x03\x12\x03+\"#\n\
    \x0c\n\x05\x04\x03\x02\x01\x08\x12\x03+$B\n\x0f\n\x08\x04\x03\x02\x01\
    \x08\xe7\x07\0\x12\x03+%A\n\x10\n\t\x04\x03\x02\x01\x08\xe7\x07\0\x02\
    \x12\x03+%9\n\x11\n\n\x04\x03\x02\x01\x08\xe7\x07\0\x02\0\x12\x03+%9\n\
    \x12\n\x0b\x04\x03\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x03+&8\n\x10\n\t\
    \x04\x03\x02\x01\x08\xe7\x07\0\x03\x12\x03+<A\n\n\n\x02\x04\x04\x12\x04.\
    \05\x01\n\n\n\x03\x04\x04\x01\x12\x03.\x08\x0e\n\x0b\n\x04\x04\x04\x02\0\
    \x12\x03/\x04K\n\x0c\n\x05\x04\x04\x02\0\x04\x12\x03/\x04\x0c\n\x0c\n\
    \x05\x04\x04\x02\0\x05\x12\x03/\r\x13\n\x0c\n\x05\x04\x04\x02\0\x01\x12\
    \x03/\x14\x16\n\x0c\n\x05\x04\x04\x02\0\x03\x12\x03/*+\n\x0c\n\x05\x04\
    \x04\x02\0\x08\x12\x03/,J\n\x0f\n\x08\x04\x04\x02\0\x08\xe7\x07\0\x12\
    \x03/-I\n\x10\n\t\x04\x04\x02\0\x08\xe7\x07\0\x02\x12\x03/-A\n\x11\n\n\
    \x04\x04\x02\0\x08\xe7\x07\0\x02\0\x12\x03/-A\n\x12\n\x0b\x04\x04\x02\0\
    \x08\xe7\x07\0\x02\0\x01\x12\x03/.@\n\x10\n\t\x04\x04\x02\0\x08\xe7\x07\
    \0\x03\x12\x03/DI\n5\n\x04\x04\x04\x02\x01\x12\x031\x04,\x1a(\x20Region\
    \x20key\x20range\x20[start_key,\x20end_key).\n\n\x0c\n\x05\x04\x04\x02\
    \x01\x04\x12\x031\x04\x0c\n\x0c\n\x05\x04\x04\x02\x01\x05\x12\x031\r\x12\
    \n\x0c\n\x05\x04\x04\x02\x01\x01\x12\x031\x14\x1d\n\x0c\n\x05\x04\x04\
    \x02\x01\x03\x12\x031*+\n\x0b\n\x04\x04\x04\x02\x02\x12\x032\x04,\n\x0c\
    \n\x05\x04\x04\x02\x02\x04\x12\x032\x04\x0c\n\x0c\n\x05\x04\x04\x02\x02\
    \x05\x12\x032\r\x12\n\x0c\n\x05\x04\x04\x02\x02\x01\x12\x032\x14\x1b\n\
    \x0c\n\x05\x04\x04\x02\x02\x03\x12\x032*+\n\x0b\n\x04\x04\x04\x02\x03\
    \x12\x033\x04,\n\x0c\n\x05\x04\x04\x02\x03\x04\x12\x033\x04\x0c\n\x0c\n\
    \x05\x04\x04\x02\x03\x06\x12\x033\r\x18\n\x0c\n\x05\x04\x04\x02\x03\x01\
    \x12\x033\x19%\n\x0c\n\x05\x04\x04\x02\x03\x03\x12\x033*+\n\x0b\n\x04\
    \x04\x04\x02\x04\x12\x034\x04,\n\x0c\n\x05\x04\x04\x02\x04\x04\x12\x034\
    \x04\x0c\n\x0c\n\x05\x04\x04\x02\x04\x06\x12\x034\r\x11\n\x0c\n\x05\x04\
    \x04\x02\x04\x01\x12\x034\x14\x19\n\x0c\n\x05\x04\x04\x02\x04\x03\x12\
    \x034*+\n\n\n\x02\x04\x05\x12\x047\0:\x01\n\n\n\x03\x04\x05\x01\x12\x037\
    \x08\x0c\n\x0b\n\x04\x04\x05\x02\0\x12\x038\x04C\n\x0c\n\x05\x04\x05\x02\
    \0\x04\x12\x038\x04\x0c\n\x0c\n\x05\x04\x05\x02\0\x05\x12\x038\r\x13\n\
    \x0c\n\x05\x04\x05\x02\0\x01\x12\x038\x14\x16\n\x0c\n\x05\x04\x05\x02\0\
    \x03\x12\x038\"#\n\x0c\n\x05\x04\x05\x02\0\x08\x12\x038$B\n\x0f\n\x08\
    \x04\x05\x02\0\x08\xe7\x07\0\x12\x038%A\n\x10\n\t\x04\x05\x02\0\x08\xe7\
    \x07\0\x02\x12\x038%9\n\x11\n\n\x04\x05\x02\0\x08\xe7\x07\0\x02\0\x12\
    \x038%9\n\x12\n\x0b\x04\x05\x02\0\x08\xe7\x07\0\x02\0\x01\x12\x038&8\n\
    \x10\n\t\x04\x05\x02\0\x08\xe7\x07\0\x03\x12\x038<A\n\x0b\n\x04\x04\x05\
    \x02\x01\x12\x039\x04C\n\x0c\n\x05\x04\x05\x02\x01\x04\x12\x039\x04\x0c\
    \n\x0c\n\x05\x04\x05\x02\x01\x05\x12\x039\r\x13\n\x0c\n\x05\x04\x05\x02\
    \x01\x01\x12\x039\x14\x1c\n\x0c\n\x05\x04\x05\x02\x01\x03\x12\x039\"#\n\
    \x0c\n\x05\x04\x05\x02\x01\x08\x12\x039$B\n\x0f\n\x08\x04\x05\x02\x01\
    \x08\xe7\x07\0\x12\x039%A\n\x10\n\t\x04\x05\x02\x01\x08\xe7\x07\0\x02\
    \x12\x039%9\n\x11\n\n\x04\x05\x02\x01\x08\xe7\x07\0\x02\0\x12\x039%9\n\
    \x12\n\x0b\x04\x05\x02\x01\x08\xe7\x07\0\x02\0\x01\x12\x039&8\n\x10\n\t\
    \x04\x05\x02\x01\x08\xe7\x07\0\x03\x12\x039<A\
";

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
