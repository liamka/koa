//! Evaluates filters directly on msgpack document bytes, with the same semantics as the JS
//! `matches` (lib/query.js) for primitive values: dot paths descend into maps, arrays met on the
//! way are walked element by element (a numeric segment picks a position), and a field matches
//! when its value, or an element of its array value, satisfies the condition: equal to one of
//! the values (equality, $in), equal to none ($ne), or ordered against a number or string
//! ($gt, $gte, $lt, $lte; values of other types never satisfy those). $elemMatch: an array
//! value with one element that meets every condition of a list, on the element itself or on
//! its fields. $size: an array value of that length. $type: a value (or an element) of one of
//! the types. $not / $nor: a list of conditions that does not hold.
//!
//! Equality compares msgpack encodings. The JS layer only sends values that encode one way
//! (strings, booleans, finite numbers), all produced by the same encoder as the documents, so
//! equal bytes means equal values. Each condition is answered yes, no, or unknown (bytes this
//! reader does not understand); a document is dropped only on a no, so the filter may let a
//! document through, never drop a matching one, negations included.
//!
//! Objects are msgpackr records: a structure id, then the field values. The field names of
//! shared structures come with the filter; other structures are defined inside the document.

use napi::bindgen_prelude::*;

const OP_EQ: u8 = 0;
const OP_GT: u8 = 1;
const OP_GTE: u8 = 2;
const OP_LT: u8 = 3;
const OP_LTE: u8 = 4;
const OP_IN: u8 = 5;
const OP_NE: u8 = 6;
const OP_OR: u8 = 7;
const OP_EXISTS: u8 = 8;
const OP_ELEM_MATCH: u8 = 9;
const OP_NOT: u8 = 10;
const OP_SIZE: u8 = 11;
const OP_TYPE: u8 = 12;

// $type codes (MongoDB's BSON type numbers) of the values documents hold
const TYPE_DOUBLE: u8 = 1;
const TYPE_STRING: u8 = 2;
const TYPE_OBJECT: u8 = 3;
const TYPE_ARRAY: u8 = 4;
const TYPE_BINARY: u8 = 5;
const TYPE_OBJECT_ID: u8 = 7;
const TYPE_BOOL: u8 = 8;
const TYPE_DATE: u8 = 9;
const TYPE_NULL: u8 = 10;
const TYPE_INT: u8 = 16;
const TYPE_LONG: u8 = 18;
/// msgpackr's timestamp extension (dates), and the ObjectId extension of lib/storage.js
const EXT_DATE: u8 = 0xff;
const EXT_OBJECT_ID: u8 = 11;

enum Test {
    /// equal to one of these encodings
    Any(Vec<Vec<u8>>),
    /// equal to none of them
    None(Vec<Vec<u8>>),
    /// ordered against a bound: the op, and the bound
    Order(u8, Bound),
    /// the path reaches a value (true), or none (false)
    Exists(bool),
    /// an array of this length (arrays in arrays are not walked)
    Size(usize),
    /// of one of these types ($type codes)
    Type(Vec<u8>),
}

enum Bound {
    Number(f64),
    String(Vec<u8>),
}

enum Condition {
    Field {
        path: Vec<Vec<u8>>,
        test: Test,
    },
    /// one of the alternatives holds: each is a list of conditions that all hold
    Or(Vec<Vec<Condition>>),
    /// a value at the path is an array with an element meeting all the conditions: tests of
    /// the element itself (`on_element`: conditions without a path, arrays not walked), or
    /// conditions on its fields (the element a document)
    ElemMatch {
        path: Vec<Vec<u8>>,
        on_element: bool,
        conditions: Vec<Condition>,
    },
    /// the conditions do not all hold
    Not(Vec<Condition>),
}

pub struct Filter {
    conditions: Vec<Condition>,
    /// field names of the shared record structures, by id - 0x40
    shared: Vec<Vec<Vec<u8>>>,
}

/// `[conditions u32]` then per condition `[op u8][segments u32]([len u32][utf-8])*[values u32]
/// ([len u32][msgpack])*`, or for $or `[7][alternatives u32]` and per alternative a list of
/// conditions as above, or for $elemMatch `[9][segments...][form u8]` then a list of conditions
/// (form 0: on the element, 1: on its fields), or for $not `[10]` then a list of conditions; then optionally the shared record structures: `[structures u32]` and
/// per structure `[fields u32]([len u32][utf-8])*`.
pub fn parse(bytes: &[u8]) -> Result<Filter> {
    let bad = || Error::from_reason("mostik: malformed native filter");
    let mut at = 0;
    let u32_at = |at: &mut usize| -> Result<usize> {
        let b = bytes.get(*at..*at + 4).ok_or_else(bad)?;
        *at += 4;
        Ok(u32::from_le_bytes(b.try_into().unwrap()) as usize)
    };
    let chunk = |at: &mut usize, len: usize| -> Result<Vec<u8>> {
        let b = bytes.get(*at..*at + len).ok_or_else(bad)?;
        *at += len;
        Ok(b.to_vec())
    };
    let strings = |at: &mut usize| -> Result<Vec<Vec<u8>>> {
        let count = u32_at(at)?;
        (0..count)
            .map(|_| {
                let len = u32_at(at)?;
                chunk(at, len)
            })
            .collect()
    };
    fn list(
        at: &mut usize,
        depth: usize,
        u32_at: &dyn Fn(&mut usize) -> Result<usize>,
        strings: &dyn Fn(&mut usize) -> Result<Vec<Vec<u8>>>,
        byte: &dyn Fn(&mut usize) -> Result<u8>,
    ) -> Result<Vec<Condition>> {
        let bad = || Error::from_reason("mostik: malformed native filter");
        if depth > 64 {
            return Err(bad());
        }
        let count = u32_at(at)?;
        let mut conditions = Vec::with_capacity(count.min(1024));
        for _ in 0..count {
            let op = byte(at)?;
            if op == OP_OR {
                let alternatives = u32_at(at)?;
                let mut or = Vec::with_capacity(alternatives.min(1024));
                for _ in 0..alternatives {
                    or.push(list(at, depth + 1, u32_at, strings, byte)?);
                }
                conditions.push(Condition::Or(or));
                continue;
            }
            if op == OP_NOT {
                conditions.push(Condition::Not(list(at, depth + 1, u32_at, strings, byte)?));
                continue;
            }
            if op == OP_ELEM_MATCH {
                let path = strings(at)?;
                let on_element = byte(at)? == 0;
                let inner = list(at, depth + 1, u32_at, strings, byte)?;
                conditions.push(Condition::ElemMatch { path, on_element, conditions: inner });
                continue;
            }
            let path = strings(at)?;
            let values = strings(at)?;
            let test = match op {
                OP_EQ | OP_IN => Test::Any(values),
                OP_NE => Test::None(values),
                OP_EXISTS => Test::Exists(values.first().and_then(|v| v.first()) == Some(&0xc3)),
                OP_SIZE => {
                    let [value] = &values[..] else {
                        return Err(bad());
                    };
                    match number(value) {
                        Some(n) if n >= 0.0 && n.fract() == 0.0 => Test::Size(n as usize),
                        _ => return Err(bad()),
                    }
                }
                OP_TYPE => Test::Type(values.iter().map(|v| number(v).map(|n| n as u8).ok_or_else(bad)).collect::<Result<_>>()?),
                OP_GT | OP_GTE | OP_LT | OP_LTE => {
                    let [value] = &values[..] else {
                        return Err(bad());
                    };
                    let bound = match (number(value), str_bytes(value)) {
                        (Some(n), _) => Bound::Number(n),
                        (_, Some(s)) => Bound::String(s.to_vec()),
                        _ => return Err(bad()),
                    };
                    Test::Order(op, bound)
                }
                _ => return Err(bad()),
            };
            conditions.push(Condition::Field { path, test });
        }
        Ok(conditions)
    }
    let byte = |at: &mut usize| -> Result<u8> {
        let b = *bytes.get(*at).ok_or_else(bad)?;
        *at += 1;
        Ok(b)
    };
    let conditions = list(&mut at, 0, &u32_at, &strings, &byte)?;
    let mut shared = Vec::new();
    if at < bytes.len() {
        let count = u32_at(&mut at)?;
        for _ in 0..count {
            shared.push(strings(&mut at)?);
        }
    }
    Ok(Filter { conditions, shared })
}

/// Keeps only some top-level fields of documents, so the caller decodes less.
pub struct Projection {
    fields: Vec<Vec<u8>>,
    shared: Vec<Vec<Vec<u8>>>,
}

/// `[fields u32]([len u32][utf-8])*` then the shared record structures, as for filters.
pub fn parse_projection(bytes: &[u8]) -> Result<Projection> {
    fn read<'b>(bytes: &'b [u8], at: &mut usize, len: usize) -> Result<&'b [u8]> {
        let b = bytes.get(*at..*at + len).ok_or_else(|| Error::from_reason("mostik: malformed projection"))?;
        *at += len;
        Ok(b)
    }
    fn count(bytes: &[u8], at: &mut usize) -> Result<usize> {
        Ok(u32::from_le_bytes(read(bytes, at, 4)?.try_into().unwrap()) as usize)
    }
    fn strings(bytes: &[u8], at: &mut usize) -> Result<Vec<Vec<u8>>> {
        let n = count(bytes, at)?;
        (0..n)
            .map(|_| {
                let len = count(bytes, at)?;
                Ok(read(bytes, at, len)?.to_vec())
            })
            .collect()
    }
    let mut at = 0;
    let fields = strings(bytes, &mut at)?;
    let n = count(bytes, &mut at)?;
    let shared = (0..n).map(|_| strings(bytes, &mut at)).collect::<Result<Vec<_>>>()?;
    Ok(Projection { fields, shared })
}

impl Projection {
    /// The document with only the wanted top-level fields, as a msgpack map; None to keep it
    /// whole (unreadable, or with structures defined inside it that a kept value might need).
    pub fn apply(&self, doc: &[u8]) -> Option<Vec<u8>> {
        let mut reader = Reader::new(&self.shared);
        let (len, mut body, names) = match reader.fields(doc)?? {
            Fields::Map { len, body } => (len, body, None),
            Fields::Record { names, body } => (names.len(), body, Some(names)),
        };
        let mut kept: Vec<(&[u8], &[u8])> = Vec::new();
        for i in 0..len {
            let name = match &names {
                Some(names) => names.at(i),
                None => {
                    let (key, rest) = reader.split_value(body)?;
                    body = rest;
                    str_bytes(key)?
                }
            };
            let (value, rest) = reader.split_value(body)?;
            body = rest;
            if self.fields.iter().any(|f| f.as_slice() == name) {
                kept.push((name, value));
            }
        }
        if !reader.own.is_empty() {
            return None;
        }
        let mut out = Vec::with_capacity(kept.iter().map(|(n, v)| n.len() + v.len() + 3).sum::<usize>() + 3);
        if kept.len() < 16 {
            out.push(0x80 | kept.len() as u8);
        } else {
            out.push(0xde);
            out.extend_from_slice(&(kept.len() as u16).to_be_bytes());
        }
        for (name, value) in kept {
            match name.len() {
                n if n < 32 => out.push(0xa0 | n as u8),
                n if n < 256 => out.extend_from_slice(&[0xd9, n as u8]),
                n => {
                    out.push(0xda);
                    out.extend_from_slice(&(n as u16).to_be_bytes());
                }
            }
            out.extend_from_slice(name);
            out.extend_from_slice(value);
        }
        Some(out)
    }
}

/// Yes, no, or unknown (None: bytes this reader cannot tell).
type Answer = Option<bool>;

/// Every one holds: no when one does not, yes when all do.
fn and(answers: impl Iterator<Item = Answer>) -> Answer {
    let mut all = Some(true);
    for answer in answers {
        match answer {
            Some(false) => return Some(false),
            None => all = None,
            Some(true) => {}
        }
    }
    all
}

/// One holds: yes when one does, no when none does.
fn or(answers: impl Iterator<Item = Answer>) -> Answer {
    let mut any = Some(false);
    for answer in answers {
        match answer {
            Some(true) => return Some(true),
            None => any = None,
            Some(false) => {}
        }
    }
    any
}

impl Filter {
    /// False only when the document certainly does not match.
    pub fn matches(&self, doc: &[u8]) -> bool {
        let mut reader = Reader::new(&self.shared);
        self.all(&mut reader, &self.conditions, doc) != Some(false)
    }

    /// Whether `value` meets every condition. Each condition reads it from its start, with the
    /// record structures known at its start (those defined before it in the document).
    fn all<'d>(&self, reader: &mut Reader<'_, 'd>, conditions: &[Condition], value: &'d [u8]) -> Answer {
        and(conditions.iter().map(|c| {
            let known = reader.own.len();
            let answer = self.one(reader, c, value);
            reader.own.truncate(known);
            answer
        }))
    }

    fn one<'d>(&self, reader: &mut Reader<'_, 'd>, condition: &Condition, doc: &'d [u8]) -> Answer {
        match condition {
            Condition::Or(alternatives) => or(alternatives.iter().map(|alternative| self.all(reader, alternative, doc))),
            Condition::Not(conditions) => self.all(reader, conditions, doc).map(|holds| !holds),
            Condition::Field { path, test } => match test {
                Test::Any(values) => reader.field_satisfies(doc, path, &|v| Some(values.iter().any(|e| e == v))),
                Test::None(values) => reader.field_satisfies(doc, path, &|v| Some(values.iter().any(|e| e == v))).map(|found| !found),
                Test::Order(op, bound) => reader.field_satisfies(doc, path, &|v| Some(ordered(v, *op, bound))),
                Test::Exists(wanted) => reader.field_satisfies(doc, path, &|_| Some(true)).map(|found| found == *wanted),
                Test::Type(types) => reader.field_satisfies(doc, path, &|v| Some(types.contains(&type_of(v)?))),
                Test::Size(n) => {
                    // the arrays the path reaches, as they are
                    let mut found = Some(false);
                    let read = reader.visit(doc, path, &mut |_, value, _| {
                        if found == Some(false) {
                            found = Some(array(value)?.is_some_and(|items| items.len == *n));
                        }
                        Some(())
                    });
                    read.and(found)
                }
            },
            Condition::ElemMatch { path, on_element, conditions } => {
                let mut found = Some(false);
                let read = reader.visit(doc, path, &mut |reader, value, _| {
                    if found != Some(true) {
                        found = or([found, self.some_element(reader, value, *on_element, conditions)].into_iter());
                    }
                    Some(())
                });
                read.and(found)
            }
        }
    }

    /// Whether `value` is an array with an element that meets `conditions`.
    fn some_element<'d>(&self, reader: &mut Reader<'_, 'd>, value: &'d [u8], on_element: bool, conditions: &[Condition]) -> Answer {
        let Some(items) = array(value)? else {
            return Some(false);
        };
        let mut body = items.body;
        let mut any = Some(false);
        for _ in 0..items.len {
            // the element is read from its start with what was known there, then skipped
            let known = reader.own.len();
            let met = if on_element {
                let (item, _) = reader.split_value(body)?;
                reader.own.truncate(known);
                self.element(reader, item, conditions)
            } else {
                let document = matches!(reader.fields(body)?, Some(_));
                reader.own.truncate(known);
                if document {
                    self.all(reader, conditions, body)
                } else {
                    Some(false)
                }
            };
            reader.own.truncate(known);
            any = or([any, met].into_iter());
            if any == Some(true) {
                return any;
            }
            body = reader.split_value(body)?.1;
        }
        any
    }

    /// Whether one element passes tests of its own (an $elemMatch on values): arrays in it are
    /// not walked.
    fn element<'d>(&self, reader: &mut Reader<'_, 'd>, item: &'d [u8], conditions: &[Condition]) -> Answer {
        and(conditions.iter().map(|c| match c {
            Condition::Field { test, .. } => match test {
                Test::Any(values) => Some(values.iter().any(|e| e == item)),
                Test::None(values) => Some(!values.iter().any(|e| e == item)),
                Test::Order(op, bound) => Some(ordered(item, *op, bound)),
                Test::Exists(wanted) => Some(*wanted),
                Test::Type(types) => Some(types.contains(&type_of(item)?)),
                Test::Size(n) => Some(array(item)?.is_some_and(|items| items.len == *n)),
            },
            Condition::ElemMatch { on_element, conditions, .. } => {
                let known = reader.own.len();
                let met = self.some_element(reader, item, *on_element, conditions);
                reader.own.truncate(known);
                met
            }
            Condition::Not(conditions) => self.element(reader, item, conditions).map(|holds| !holds),
            Condition::Or(_) => None,
        }))
    }
}

/// The $type code of a msgpack value as lib/query.js sees it once decoded: integers of 32 bits
/// (whole floats too) are int, other numbers double, 64-bit integers long (they decode as
/// BigInt); 0 for values of no such type (other extensions); None when unreadable.
fn type_of(value: &[u8]) -> Option<u8> {
    let (&tag, rest) = value.split_first()?;
    let int32 = |n: f64| n.fract() == 0.0 && n >= i32::MIN as f64 && n <= i32::MAX as f64;
    Some(match tag {
        0x00..=0x3f | 0xe0..=0xff | 0xcc | 0xcd | 0xd0 | 0xd1 | 0xd2 => TYPE_INT,
        0xce | 0xca | 0xcb => {
            if int32(number(value)?) {
                TYPE_INT
            } else {
                TYPE_DOUBLE
            }
        }
        0xcf | 0xd3 => TYPE_LONG,
        0xc0 => TYPE_NULL,
        0xc2 | 0xc3 => TYPE_BOOL,
        0xa0..=0xbf | 0xd9 | 0xda | 0xdb => TYPE_STRING,
        0xc4..=0xc6 => TYPE_BINARY,
        0x90..=0x9f | 0xdc | 0xdd => TYPE_ARRAY,
        0x80..=0x8f | 0xde | 0xdf => TYPE_OBJECT,
        _ if RECORDS.contains(&tag) => TYPE_OBJECT,
        0xd4 if rest.first() == Some(&RECORD_DEFINITION) => TYPE_OBJECT,
        // extensions: the type byte after the length, if any
        0xd4..=0xd8 | 0xc7..=0xc9 => {
            let at = match tag {
                0xc7 => 1,
                0xc8 => 2,
                0xc9 => 4,
                _ => 0,
            };
            match *rest.get(at)? {
                EXT_DATE => TYPE_DATE,
                EXT_OBJECT_ID => TYPE_OBJECT_ID,
                _ => 0,
            }
        }
        _ => 0,
    })
}

/// First byte of a record: its structure id (msgpackr's record extension; positive fixints
/// 64-127 are written as uint8 instead).
const RECORDS: std::ops::RangeInclusive<u8> = 0x40..=0x7f;
/// msgpackr's record definition extension type
const RECORD_DEFINITION: u8 = 0x72;

/// Reads the values of one document. Record structures that are not shared are defined inside
/// the document, before their first use; the reader learns them as it goes (every value before
/// a position is read or skipped before that position is reached).
struct Reader<'a, 'd> {
    shared: &'a [Vec<Vec<u8>>],
    /// structures defined in the document so far: (id, field names)
    own: Vec<(u8, Vec<&'d [u8]>)>,
}

struct Items<'a> {
    len: usize,
    body: &'a [u8],
}

/// Keys and values of a map or record.
enum Fields<'a, 'd> {
    Map { len: usize, body: &'d [u8] },
    Record { names: Names<'a, 'd>, body: &'d [u8] },
}

enum Names<'a, 'd> {
    Shared(&'a [Vec<u8>]),
    Own(Vec<&'d [u8]>),
}

impl Names<'_, '_> {
    fn len(&self) -> usize {
        match self {
            Names::Shared(names) => names.len(),
            Names::Own(names) => names.len(),
        }
    }

    fn at(&self, i: usize) -> &[u8] {
        match self {
            Names::Shared(names) => &names[i],
            Names::Own(names) => names[i],
        }
    }
}

impl<'a, 'd> Reader<'a, 'd> {
    fn new(shared: &'a [Vec<Vec<u8>>]) -> Self {
        Reader { shared, own: Vec::new() }
    }

    /// Whether a value the path reaches, or an element of one that is an array, passes `test`
    /// (None from `test`: it cannot tell). None: the bytes could not be read.
    fn field_satisfies(&mut self, doc: &'d [u8], path: &[Vec<u8>], test: &dyn Fn(&[u8]) -> Option<bool>) -> Answer {
        let mut found = Some(false);
        self.visit(doc, path, &mut |reader, value, by_position| {
            if found == Some(true) {
                return Some(());
            }
            found = or([found, test(value)].into_iter());
            // an array the path ends at by a position is a whole value, its elements are not
            if found != Some(true) && !by_position {
                if let Some(items) = array(value)? {
                    let mut rest = items.body;
                    for _ in 0..items.len {
                        let (item, next) = reader.split_value(rest)?;
                        found = or([found, test(item)].into_iter());
                        if found == Some(true) {
                            break;
                        }
                        rest = next;
                    }
                }
            }
            Some(())
        })?;
        found
    }

    /// Calls `f` with every value the path reaches from `value` (lookup() in lib/query.js), and
    /// whether the path ends there by a position in an array (an array there then matches as a
    /// whole value only).
    fn visit(&mut self, value: &'d [u8], path: &[Vec<u8>], f: &mut dyn FnMut(&mut Self, &'d [u8], bool) -> Option<()>) -> Option<()> {
        self.walk(value, path, false, f)
    }

    fn walk(&mut self, value: &'d [u8], path: &[Vec<u8>], by_position: bool, f: &mut dyn FnMut(&mut Self, &'d [u8], bool) -> Option<()>) -> Option<()> {
        let Some((segment, rest)) = path.split_first() else {
            return f(self, value, by_position);
        };
        if let Some(items) = array(value)? {
            let position = std::str::from_utf8(segment).ok().filter(|s| s.bytes().all(|b| b.is_ascii_digit()) && !s.is_empty());
            let mut body = items.body;
            for i in 0..items.len {
                let (item, next) = self.split_value(body)?;
                // a numeric segment picks that position of the array, and the field of that name
                // in its documents
                if position.is_some_and(|p| p.parse::<usize>().ok() == Some(i)) {
                    self.walk(item, rest, true, f)?;
                }
                if let Some(field) = self.field(item, segment)? {
                    self.walk(field, rest, false, f)?;
                }
                body = next;
            }
            return Some(());
        }
        if let Some(field) = self.field(value, segment)? {
            self.walk(field, rest, false, f)?;
        }
        Some(())
    }

    /// The fields of a map or record value; Some(None) when the value is neither.
    fn fields(&mut self, value: &'d [u8]) -> Option<Option<Fields<'a, 'd>>> {
        let (&tag, rest) = value.split_first()?;
        Some(Some(match tag {
            0x80..=0x8f => Fields::Map { len: (tag & 0x0f) as usize, body: rest },
            0xde => Fields::Map { len: be(rest, 2)?, body: rest.get(2..)? },
            0xdf => Fields::Map { len: be(rest, 4)?, body: rest.get(4..)? },
            _ if RECORDS.contains(&tag) => {
                let names = match self.own.iter().rev().find(|(id, _)| *id == tag) {
                    Some((_, names)) => Names::Own(names.clone()),
                    None => Names::Shared(self.shared.get((tag - 0x40) as usize)?),
                };
                Fields::Record { names, body: rest }
            }
            0xd4 if rest.first() == Some(&RECORD_DEFINITION) => {
                // a structure defined here, then the record's values
                let id = *rest.get(1)?;
                if !RECORDS.contains(&id) {
                    return None;
                }
                let items = array(rest.get(2..)?)??;
                let (mut names, mut body) = (Vec::with_capacity(items.len), items.body);
                for _ in 0..items.len {
                    let (name, next) = self.split_value(body)?;
                    names.push(str_bytes(name)?);
                    body = next;
                }
                self.own.push((id, names.clone()));
                Fields::Record { names: Names::Own(names), body }
            }
            _ => return Some(None),
        }))
    }

    /// The value under string key `key` when `value` is a map or record; Some(None) when absent
    /// or neither.
    fn field(&mut self, value: &'d [u8], key: &[u8]) -> Option<Option<&'d [u8]>> {
        match self.fields(value)? {
            None => Some(None),
            Some(Fields::Map { len, mut body }) => {
                for _ in 0..len {
                    let (k, next) = self.split_value(body)?;
                    let (v, next) = self.split_value(next)?;
                    if str_bytes(k) == Some(key) {
                        return Some(Some(v));
                    }
                    body = next;
                }
                Some(None)
            }
            Some(Fields::Record { names, mut body }) => {
                for i in 0..names.len() {
                    let (v, next) = self.split_value(body)?;
                    if names.at(i) == key {
                        return Some(Some(v));
                    }
                    body = next;
                }
                Some(None)
            }
        }
    }

    /// Splits the first msgpack value off `bytes`: (value, rest).
    fn split_value(&mut self, bytes: &'d [u8]) -> Option<(&'d [u8], &'d [u8])> {
        let len = self.value_len(bytes)?;
        (len <= bytes.len()).then(|| bytes.split_at(len))
    }

    /// Encoded length of the msgpack value at the start of `bytes`.
    fn value_len(&mut self, bytes: &'d [u8]) -> Option<usize> {
        let (&tag, rest) = bytes.split_first()?;
        let len = match tag {
            0x00..=0x3f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => 1,
            0xa0..=0xbf => 1 + (tag & 0x1f) as usize,
            0xcc | 0xd0 => 2,
            0xcd | 0xd1 => 3,
            0xca | 0xce | 0xd2 => 5,
            0xcb | 0xcf | 0xd3 => 9,
            0xd9 | 0xc4 => 2 + be(rest, 1)?,
            0xda | 0xc5 => 3 + be(rest, 2)?,
            0xdb | 0xc6 => 5 + be(rest, 4)?,
            0xd4 if rest.first() != Some(&RECORD_DEFINITION) => 3,
            0xd5 if rest.first() == Some(&RECORD_DEFINITION) => return None, // two-byte ids: not used
            0xd5 => 4,
            0xd6 => 6,
            0xd7 => 10,
            0xd8 => 18,
            0xc7 => 3 + be(rest, 1)?,
            0xc8 => 4 + be(rest, 2)?,
            0xc9 => 6 + be(rest, 4)?,
            0x90..=0x9f | 0xdc | 0xdd => {
                let items = array(bytes)??;
                let mut body = items.body;
                for _ in 0..items.len {
                    body = self.split_value(body)?.1;
                }
                bytes.len() - body.len()
            }
            _ => {
                // maps and records (0x40-0x7f, or a definition then the record)
                let (count, mut body) = match self.fields(bytes)?? {
                    Fields::Map { len, body } => (2 * len, body),
                    Fields::Record { names, body } => (names.len(), body),
                };
                for _ in 0..count {
                    body = self.split_value(body)?.1;
                }
                bytes.len() - body.len()
            }
        };
        Some(len)
    }
}

/// The elements of an array value; Some(None) when the value is not an array.
fn array(value: &[u8]) -> Option<Option<Items<'_>>> {
    let (&tag, rest) = value.split_first()?;
    let (len, body) = match tag {
        0x90..=0x9f => ((tag & 0x0f) as usize, rest),
        0xdc => (be(rest, 2)?, rest.get(2..)?),
        0xdd => (be(rest, 4)?, rest.get(4..)?),
        _ => return Some(None),
    };
    Some(Some(Items { len, body }))
}

fn str_bytes(value: &[u8]) -> Option<&[u8]> {
    let (&tag, rest) = value.split_first()?;
    let (len, body) = match tag {
        0xa0..=0xbf => ((tag & 0x1f) as usize, rest),
        0xd9 => (be(rest, 1)?, rest.get(1..)?),
        0xda => (be(rest, 2)?, rest.get(2..)?),
        0xdb => (be(rest, 4)?, rest.get(4..)?),
        _ => return None,
    };
    body.get(..len)
}

/// Whether msgpack `value` stands in relation `op` to `bound`: numbers against numbers, strings
/// against strings (by bytes, i.e. code points); nothing else compares. NaN only equals NaN.
fn ordered(value: &[u8], op: u8, bound: &Bound) -> bool {
    let order = match bound {
        Bound::Number(b) => {
            let Some(v) = number(value) else { return false };
            if v.is_nan() || b.is_nan() {
                if !(v.is_nan() && b.is_nan()) {
                    return false;
                }
                std::cmp::Ordering::Equal
            } else {
                v.partial_cmp(b).unwrap()
            }
        }
        Bound::String(b) => match str_bytes(value) {
            Some(v) => v.cmp(b.as_slice()),
            None => return false,
        },
    };
    match op {
        OP_GT => order.is_gt(),
        OP_GTE => order.is_ge(),
        OP_LT => order.is_lt(),
        _ => order.is_le(),
    }
}

/// The number a msgpack value holds, if it is one (0x40-0x7f are record ids here, not integers).
fn number(value: &[u8]) -> Option<f64> {
    let (&tag, rest) = value.split_first()?;
    let int = |n: usize| -> Option<u64> { Some(be(rest, n)? as u64) };
    Some(match tag {
        0x00..=0x3f => tag as f64,
        0xe0..=0xff => (tag as i8) as f64,
        0xcc => int(1)? as f64,
        0xcd => int(2)? as f64,
        0xce => int(4)? as f64,
        0xcf => u64::from_be_bytes(rest.get(..8)?.try_into().ok()?) as f64,
        0xd0 => (int(1)? as u8 as i8) as f64,
        0xd1 => (int(2)? as u16 as i16) as f64,
        0xd2 => (int(4)? as u32 as i32) as f64,
        0xd3 => i64::from_be_bytes(rest.get(..8)?.try_into().ok()?) as f64,
        0xca => f32::from_be_bytes(rest.get(..4)?.try_into().ok()?) as f64,
        0xcb => f64::from_be_bytes(rest.get(..8)?.try_into().ok()?),
        _ => return None,
    })
}

fn be(bytes: &[u8], n: usize) -> Option<usize> {
    Some(bytes.get(..n)?.iter().fold(0usize, |acc, &b| (acc << 8) | b as usize))
}

/// One field's value for sorting, in MongoDB's order of types (the variants' order); values of
/// other types are not ordered here.
#[derive(Clone, Debug)]
pub enum SortValue {
    EmptyArray,
    Null,
    /// NaN below every other number
    Number(f64),
    String(Vec<u8>),
    ObjectId([u8; 12]),
    Bool(bool),
    /// seconds, nanoseconds
    Date(i64, u32),
}

impl SortValue {
    fn rank(&self) -> u8 {
        match self {
            SortValue::EmptyArray => 0,
            SortValue::Null => 1,
            SortValue::Number(_) => 2,
            SortValue::String(_) => 3,
            SortValue::ObjectId(_) => 7,
            SortValue::Bool(_) => 8,
            SortValue::Date(..) => 9,
        }
    }
}

impl Ord for SortValue {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        use SortValue::*;
        match (self, other) {
            (Number(a), Number(b)) => match (a.is_nan(), b.is_nan()) {
                (true, true) => std::cmp::Ordering::Equal,
                (true, false) => std::cmp::Ordering::Less,
                (false, true) => std::cmp::Ordering::Greater,
                // -0 equals 0
                _ => a.partial_cmp(b).unwrap(),
            },
            (String(a), String(b)) => a.cmp(b),
            (ObjectId(a), ObjectId(b)) => a.cmp(b),
            (Bool(a), Bool(b)) => a.cmp(b),
            (Date(a, x), Date(b, y)) => (a, x).cmp(&(b, y)),
            _ => self.rank().cmp(&other.rank()),
        }
    }
}

impl PartialOrd for SortValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for SortValue {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other).is_eq()
    }
}

impl Eq for SortValue {}

/// A sort value of a msgpack value that is not an array; None for other types.
fn sort_value(value: &[u8]) -> Option<SortValue> {
    let (&tag, rest) = value.split_first()?;
    if let Some(s) = str_bytes(value) {
        return Some(SortValue::String(s.to_vec()));
    }
    // 64-bit integers read back as BigInt, which lib/query.js orders apart from numbers
    if tag == 0xcf || tag == 0xd3 {
        return None;
    }
    if let Some(n) = number(value) {
        return Some(SortValue::Number(n));
    }
    Some(match tag {
        0xc0 => SortValue::Null,
        0xc2 => SortValue::Bool(false),
        0xc3 => SortValue::Bool(true),
        // msgpackr's dates: 32-bit seconds; 64-bit nanoseconds and seconds; 96-bit
        0xd6 if rest.first() == Some(&EXT_DATE) => SortValue::Date(be(rest.get(1..)?, 4)? as i64, 0),
        0xd7 if rest.first() == Some(&EXT_DATE) => {
            let n = u64::from_be_bytes(rest.get(1..9)?.try_into().ok()?);
            SortValue::Date((n & 0x3_ffff_ffff) as i64, (n >> 34) as u32)
        }
        0xc7 if rest.first() == Some(&12) && rest.get(1) == Some(&EXT_DATE) => {
            let nanos = u32::from_be_bytes(rest.get(2..6)?.try_into().ok()?);
            SortValue::Date(i64::from_be_bytes(rest.get(6..14)?.try_into().ok()?), nanos)
        }
        0xc7 if rest.first() == Some(&12) && rest.get(1) == Some(&EXT_OBJECT_ID) => SortValue::ObjectId(rest.get(2..14)?.try_into().ok()?),
        _ => return None,
    })
}

impl Filter {
    /// The keys `doc` sorts by on `paths` (true: descending), as lib/query.js sortKeys: the
    /// smallest value there ascending, the largest descending, array elements one by one, an empty
    /// array below null, null when missing. None when this cannot tell: a value of another type,
    /// arrays under two of the paths (MongoDB may refuse the sort), unreadable bytes.
    pub fn sort_keys(&self, doc: &[u8], paths: &[(Vec<Vec<u8>>, bool)]) -> Option<Vec<SortValue>> {
        let mut reader = Reader::new(&self.shared);
        let mut keys = Vec::with_capacity(paths.len());
        let mut arrays = 0;
        for (path, descending) in paths {
            let known = reader.own.len();
            let mut best: Option<SortValue> = None;
            let mut in_array = false;
            let mut other = false;
            let mut consider = |value: SortValue| {
                let better = match &best {
                    None => true,
                    Some(b) => {
                        if *descending {
                            value > *b
                        } else {
                            value < *b
                        }
                    }
                };
                if better {
                    best = Some(value);
                }
            };
            reader.visit(doc, path, &mut |reader, value, _| {
                match array(value)? {
                    None => match sort_value(value) {
                        Some(v) => consider(v),
                        None => other = true,
                    },
                    Some(items) => {
                        in_array = true;
                        if items.len == 0 {
                            consider(SortValue::EmptyArray);
                        }
                        let mut body = items.body;
                        for _ in 0..items.len {
                            let (item, next) = reader.split_value(body)?;
                            match sort_value(item) {
                                Some(v) => consider(v),
                                None => other = true,
                            }
                            body = next;
                        }
                    }
                }
                Some(())
            })?;
            reader.own.truncate(known);
            // a path through an array (not only ending at one) counts too: through documents in it
            if in_array || path.len() > 1 && through_array(&mut reader, doc, path)? {
                arrays += 1;
            }
            reader.own.truncate(known);
            if other || (arrays > 1 && paths.len() > 1) {
                return None;
            }
            keys.push(best.unwrap_or(SortValue::Null));
        }
        Some(keys)
    }
}

/// Whether a prefix of `path` shorter than it reaches an array through documents.
fn through_array<'d>(reader: &mut Reader<'_, 'd>, doc: &'d [u8], path: &[Vec<u8>]) -> Option<bool> {
    let mut value = doc;
    for segment in &path[..path.len() - 1] {
        match reader.field(value, segment)? {
            None => return Some(false),
            Some(next) => {
                if array(next)?.is_some() {
                    return Some(true);
                }
                value = next;
            }
        }
    }
    Some(false)
}

impl SortValue {
    /// Bytes that order as the values do (MongoDB's order): a type byte, then the value. None
    /// is a prefix of another, so a field's bytes can be inverted for a descending order.
    pub fn encode(&self, out: &mut Vec<u8>) {
        match self {
            SortValue::EmptyArray => out.push(0x10),
            SortValue::Null => out.push(0x20),
            SortValue::Number(n) => {
                out.push(0x30);
                if n.is_nan() {
                    // below every other number
                    out.extend_from_slice(&[0; 9]);
                } else {
                    // -0 is 0; the sign bit flipped, and every bit of a negative number
                    let bits = (if *n == 0.0 { 0.0 } else { *n }).to_bits();
                    let ordered = if bits >> 63 == 1 { !bits } else { bits | 1 << 63 };
                    out.push(1);
                    out.extend_from_slice(&ordered.to_be_bytes());
                }
            }
            SortValue::String(bytes) => {
                out.push(0x40);
                // a zero byte is written 0 0xff, and the string ends with 0 0
                for &b in bytes {
                    out.push(b);
                    if b == 0 {
                        out.push(0xff);
                    }
                }
                out.extend_from_slice(&[0, 0]);
            }
            SortValue::ObjectId(id) => {
                out.push(0x70);
                out.extend_from_slice(id);
            }
            SortValue::Bool(b) => out.extend_from_slice(&[0x80, *b as u8]),
            SortValue::Date(seconds, nanos) => {
                out.push(0x90);
                out.extend_from_slice(&((*seconds as u64) ^ (1 << 63)).to_be_bytes());
                out.extend_from_slice(&nanos.to_be_bytes());
            }
        }
    }
}

/// The bytes of `keys` in order, each field's inverted when it is `descending`.
pub fn encode_sort_keys(keys: &[SortValue], descending: &[bool], out: &mut Vec<u8>) {
    for (key, &down) in keys.iter().zip(descending) {
        let start = out.len();
        key.encode(out);
        if down {
            for b in &mut out[start..] {
                *b = !*b;
            }
        }
    }
}

impl Filter {
    /// The value at `path` through documents only, as a JS field path reads it for $group:
    /// Some(None) when missing (or a value that is no document on the way), None when an array
    /// is met (arrays are for the JS side) or the bytes cannot be read.
    pub fn plain_path<'d>(&self, doc: &'d [u8], path: &[Vec<u8>]) -> Option<Option<&'d [u8]>> {
        let mut reader = Reader::new(&self.shared);
        let mut value = doc;
        for segment in path {
            if array(value)?.is_some() {
                return None;
            }
            match reader.field(value, segment)? {
                Some(next) => value = next,
                None => return Some(None),
            }
        }
        if array(value)?.is_some() {
            return None;
        }
        Some(Some(value))
    }
}

/// A value's sort value when it is a plain one (no document, array or 64-bit integer).
pub fn plain_sort_value(value: &[u8]) -> Option<SortValue> {
    sort_value(value)
}

/// The number a msgpack value holds as JS reads it (not 64-bit integers, which are BigInt).
pub fn js_number(value: &[u8]) -> Option<f64> {
    match value.first() {
        Some(0xcf | 0xd3) => None,
        _ => number(value),
    }
}
