use anyhow::Result;
use const_hex::FromHex;

pub fn preprocessing<T: FromHex>(l2_hash: &str, l1_head_hash: &str) -> Result<(T, T, String)>
where
    <T as FromHex>::Error: std::fmt::Display,
{
    let user_req_id = format!(
        "{}-{}",
        l2_hash.chars().take(8).collect::<String>(),
        l1_head_hash.chars().take(8).collect::<String>()
    );
    let l2_hash = b256_from_str::<T>(l2_hash)?;
    let l1_head_hash = b256_from_str::<T>(l1_head_hash)?;

    Ok((l2_hash, l1_head_hash, user_req_id))
}

pub fn b256_from_str<T: FromHex>(s: &str) -> Result<T>
where
    <T as FromHex>::Error: std::fmt::Display,
{
    match T::from_hex(s.strip_prefix("0x").unwrap_or(s)) {
        Ok(b) => Ok(b),
        Err(e) => Err(anyhow::anyhow!("failed to parse B256 from string: {}", e)),
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{hex::FromHex, B256};

    use crate::types::preprocessing;

    #[test]
    fn test_preprocessing() {
        let expected_l2_hash = "abef4fd64a81faadb0e5e968f28353c10227c04ec0e14068ffd0a91143185267";
        let expected_l1_head_hash =
            "30da309b674ec5e6fad3f09a6065623816b029a93edbabfb5376d1dfed5e08d7";
        let (l2_hash, l1_head_hash, user_req_id) =
            preprocessing::<B256>(expected_l2_hash, expected_l1_head_hash).unwrap();

        assert_eq!(l2_hash, B256::from_hex(expected_l2_hash).unwrap());
        assert_eq!(l1_head_hash, B256::from_hex(expected_l1_head_hash).unwrap());
        assert_eq!(user_req_id, "abef4fd6-30da309b");
    }
}
