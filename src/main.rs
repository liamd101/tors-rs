use std::collections::HashMap;

#[derive(Debug)]
enum BenCodingError {
    NullRoot,
    NonSingularRoot,
    InvalidType,
    MissingTerminator,
    String(BenCodingStringError),
    Int(BenCodingIntError),
    Dict(BenCodingDictError),
}
#[derive(Debug)]
enum BenCodingStringError {
    NegativeLength,
    MissingColon,
    EOF,
    NonAsciiLength,
}
#[derive(Debug)]
enum BenCodingIntError {
    NaN,
    LeadingZero,
    NegativeZero,
}
#[derive(Debug)]
enum BenCodingDictError {
    InvalidKey,
    DuplicateKey,
    UnsortedKeys,
    MissingValue,
}

#[derive(Debug)]
enum BenCodedValue {
    String(String),
    Int(i64),
    List(Vec<BenCodedValue>),
    Dict(HashMap<String, BenCodedValue>),
}

fn decode_string(encoded: &String) -> Result<(BenCodedValue, String), BenCodingError> {
    let Some((l, rest)) = encoded.split_once(":") else {
        return Err(BenCodingError::String(BenCodingStringError::MissingColon));
    };
    if l.strip_prefix('-').is_some() {
        return Err(BenCodingError::String(BenCodingStringError::NegativeLength));
    }
    let Ok(len) = l.parse::<usize>() else {
        println!("l={}", l);
        println!("rest={}", rest);
        return Err(BenCodingError::String(BenCodingStringError::NonAsciiLength));
    };
    if len > rest.len() {
        println!("len={}", len);
        return Err(BenCodingError::String(BenCodingStringError::EOF));
    }
    let (val, rest) = rest.split_at(len);
    Ok((BenCodedValue::String(val.into()), rest.into()))
}

fn decode_integer(encoded: &String) -> Result<(BenCodedValue, String), BenCodingError> {
    let Some(encoded) = encoded.strip_prefix('i') else {
        return Err(BenCodingError::InvalidType);
    };
    let Some((digits, rest)) = encoded.split_once('e') else {
        return Err(BenCodingError::MissingTerminator);
    };
    let Ok(parsed) = digits.parse::<i64>() else {
        return Err(BenCodingError::Int(BenCodingIntError::NaN));
    };
    if parsed == 0 && digits.starts_with('-') {
        Err(BenCodingError::Int(BenCodingIntError::NegativeZero))
    } else if parsed == 0 && digits.len() > 1 {
        Err(BenCodingError::Int(BenCodingIntError::LeadingZero))
    } else {
        Ok((BenCodedValue::Int(parsed), rest.into()))
    }
}

fn decode_list(encoded: &String) -> Result<(BenCodedValue, String), BenCodingError> {
    let Some(rest) = encoded.strip_prefix('l') else {
        return Err(BenCodingError::InvalidType);
    };
    let mut rest = rest.to_string();
    let mut vals: Vec<BenCodedValue> = vec![];
    while rest.strip_prefix('e').is_none() {
        if rest.is_empty() {
            return Err(BenCodingError::MissingTerminator);
        }
        let result = match rest.chars().next() {
            None => return Err(BenCodingError::MissingTerminator),
            Some('i') => decode_integer(&rest),
            Some('0'..='9') => decode_string(&rest),
            Some('l') => decode_list(&rest),
            Some('d') => decode_dict(&rest),
            _ => return Err(BenCodingError::InvalidType),
        };
        if let Ok((val, r)) = result {
            vals.push(val);
            rest = r;
        } else {
            return result;
        }
    }
    let Some(end) = rest.strip_prefix('e') else {
        return Err(BenCodingError::MissingTerminator);
    };
    Ok((BenCodedValue::List(vals), end.into()))
}

fn decode_dict(encoded: &String) -> Result<(BenCodedValue, String), BenCodingError> {
    let Some(rest) = encoded.strip_prefix('d') else {
        return Err(BenCodingError::InvalidType);
    };
    let mut rest = rest.to_string();
    let mut vals: HashMap<String, BenCodedValue> = HashMap::new();
    while rest.strip_prefix('e').is_none() {
        if rest.is_empty() {
            return Err(BenCodingError::MissingTerminator);
        }

        let Ok((key, r)) = decode_string(&rest) else {
            return Err(BenCodingError::Dict(BenCodingDictError::InvalidKey));
        };
        let BenCodedValue::String(key) = key else {
            return Err(BenCodingError::Dict(BenCodingDictError::InvalidKey));
        };

        let result = match r.chars().next() {
            None => return Err(BenCodingError::MissingTerminator),
            Some('i') => decode_integer(&r),
            Some('0'..='9') => decode_string(&r),
            Some('l') => decode_list(&r),
            Some('d') => decode_dict(&r),
            _ => return Err(BenCodingError::InvalidType),
        };
        if let Ok((val, r)) = result {
            if vals.insert(key, val).is_some() {
                return Err(BenCodingError::Dict(BenCodingDictError::DuplicateKey));
            }
            rest = r;
        } else {
            return result;
        }
    }
    let Some(end) = rest.strip_prefix('e') else {
        return Err(BenCodingError::MissingTerminator);
    };
    Ok((BenCodedValue::Dict(vals), end.into()))
}

fn main() {
    // let testing_string: String = "4:clamp".into();
    let testing_string: String = "d7:meaningi42e4:wiki7:bencodee".into();
    match decode_dict(&testing_string) {
        Ok((decoded, rest)) => {
            println!("Decoded value: {:?}", decoded);
            println!("Remaining part of string: {}", rest);
        }
        Err(e) => {
            println!("could not decode string: {:?}", e);
        }
    };
}
