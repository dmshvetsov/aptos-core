/// A coin example with fine-grained control by multisig admin account.
module example_addr::multisig_managed_coin {
    use aptos_framework::fungible_asset::{MintRef, BurnRef, TransferRef, FungibleAsset, Metadata, FungibleStore};
    use aptos_framework::fungible_asset;
    use aptos_framework::object;
    use aptos_framework::primary_fungible_store;
    use std::option;
    use aptos_framework::object::{Object, ObjectCore};
    use std::vector;
    use std::signer;
    use std::error;
    use aptos_framework::multisig_account;
    use std::string::utf8;

    /// Only fungible asset metadata owner can make changes.
    const ENOT_OWNER: u64 = 1;

    /// The asset symbol which should be customized.
    const ASSET_SYMBOL: vector<u8> = b"MEME";

    #[resource_group_member(group = aptos_framework::object::ObjectGroup)]
    /// Hold refs to control the minting, transfer and burning of fungible assets.
    struct ManagingRefs has key {
        mint_ref: MintRef,
        transfer_ref: TransferRef,
        burn_ref: BurnRef,
    }

    fun init_module(creator: &signer) {
        let multisig_address = multisig_account::get_next_multisig_account_address(signer::address_of(creator));
        // You should customize those arguments per your need.
        multisig_account::create_with_owners(
            creator,
            vector[@0x123, @0x456],
            2,
            vector[],
            vector[],
        );

        let constructor_ref = &object::create_named_object(creator, ASSET_SYMBOL);

        // The ideal way is to get the multisig account signer but it is unavailable right now. So the pattern is to
        // create the metadata object using creator addresss as a partial seed and transfer it to multisig account.
        object::transfer(creator, object::object_from_constructor_ref<ObjectCore>(constructor_ref), multisig_address);

        // You should customize those arguments per your need.
        primary_fungible_store::create_primary_store_enabled_fungible_asset(
            constructor_ref,
            option::none(), // maximum supply
            utf8(b"meme coin"),
            utf8(b"meme"),
            8,
            utf8(b"http://meme.xyz/favicon.ico"),
            utf8(b"http://meme.xyz/"),
        );

        // Create mint/burn/transfer refs to allow creator to manage the fungible asset.
        let mint_ref = fungible_asset::generate_mint_ref(constructor_ref);
        let burn_ref = fungible_asset::generate_burn_ref(constructor_ref);
        let transfer_ref = fungible_asset::generate_transfer_ref(constructor_ref);
        let metadata_object_signer = object::generate_signer(constructor_ref);
        move_to(
            &metadata_object_signer,
            ManagingRefs { mint_ref, transfer_ref, burn_ref }
        )
    }

    #[view]
    /// Return the address of the managed fungible asset that's created when this module is deployed.
    public fun get_metadata(): Object<Metadata> {
        let asset_address = object::create_object_address(&@example_addr, ASSET_SYMBOL);
        object::address_to_object<Metadata>(asset_address)
    }

    /// Mint as the owner of metadata object to the primary fungible store of `to` account.
    public entry fun mint_to_primary_store(admin: &signer, to: address, amount: u64) acquires ManagingRefs {
        let asset = get_metadata();
        let receiver_primary_store = primary_fungible_store::ensure_primary_store_exists(to, asset);
        mint(admin, receiver_primary_store, amount);
    }

    public entry fun mint(
        admin: &signer,
        fungible_store: Object<FungibleStore>,
        amount: u64,
    ) acquires ManagingRefs {
        let asset = get_metadata();
        let managing_refs = authorized_borrow_refs(admin, asset);
        let fa = fungible_asset::mint(&managing_refs.mint_ref, amount);
        fungible_asset::deposit_with_ref(&managing_refs.transfer_ref, fungible_store, fa);
    }

    /// Transfer as the owner of metadata object ignoring `frozen` field between the primary stores of two accounts
    public entry fun transfer_between_primary_stores(
        admin: &signer,
        from: address,
        to: address,
        amount: u64
    ) acquires ManagingRefs {
        let asset = get_metadata();
        let sender_primary_store = primary_fungible_store::ensure_primary_store_exists(from, asset);
        let receiver_primary_store = primary_fungible_store::ensure_primary_store_exists(to, asset);
        transfer(admin, sender_primary_store, receiver_primary_store, amount);
    }

    /// Transfer as the owner of metadata object ignoring `frozen` field between two fungible stores.
    public entry fun transfer(
        admin: &signer,
        sender_store: Object<FungibleStore>,
        receiver_store: Object<FungibleStore>,
        amount: u64,
    ) acquires ManagingRefs {
        let asset = get_metadata();
        let managing_refs = authorized_borrow_refs(admin, asset);
        fungible_asset::transfer_with_ref(&managing_refs.transfer_ref, sender_store, receiver_store, amount);
    }

    /// Burn fungible assets as the owner of metadata object from the primary store of an account.
    public entry fun burn_from_primary_store(admin: &signer, from: address, amount: u64) acquires ManagingRefs {
        let asset = get_metadata();
        let primary_store = primary_fungible_store::ensure_primary_store_exists(from, asset);
        burn(admin, primary_store, amount);
    }

    /// Burn fungible assets as the owner of metadata object from a fungible store.
    public entry fun burn(
        admin: &signer,
        store: Object<FungibleStore>,
        amount: u64
    ) acquires ManagingRefs {
        let asset = get_metadata();
        let burn_ref = &authorized_borrow_refs(admin, asset).burn_ref;
        fungible_asset::burn_from(burn_ref, store, amount);
    }

    /// Freeze the primary stores of accounts so they cannot transfer or receive fungible assets.
    public entry fun set_primary_stores_frozen_status(
        admin: &signer,
        accounts: vector<address>,
        frozen: bool
    ) acquires ManagingRefs {
        let asset = get_metadata();
        let primary_stores = vector::map(accounts, |acct| {
            primary_fungible_store::ensure_primary_store_exists(acct, asset)
        });
        set_frozen_status(admin, primary_stores, frozen);
    }

    /// Freeze the primary store of an account so it cannot transfer or receive fungible assets.
    public entry fun set_frozen_status(
        admin: &signer,
        stores: vector<Object<FungibleStore>>,
        frozen: bool
    ) acquires ManagingRefs {
        let asset = get_metadata();
        let transfer_ref = &authorized_borrow_refs(admin, asset).transfer_ref;
        vector::for_each(stores, |store| {
            fungible_asset::set_frozen_flag(transfer_ref, store, frozen);
        });
    }

    /// Withdraw as the owner of metadata object ignoring `frozen` field.
    public fun withdraw(admin: &signer, amount: u64, from: address): FungibleAsset acquires ManagingRefs {
        let asset = get_metadata();
        let transfer_ref = &authorized_borrow_refs(admin, asset).transfer_ref;
        let from_wallet = primary_fungible_store::ensure_primary_store_exists(from, asset);
        fungible_asset::withdraw_with_ref(transfer_ref, from_wallet, amount)
    }

    /// Deposit as the owner of metadata object ignoring `frozen` field.
    public fun deposit(admin: &signer, to: address, fa: FungibleAsset) acquires ManagingRefs {
        let asset = get_metadata();
        let transfer_ref = &authorized_borrow_refs(admin, asset).transfer_ref;
        let receiver_primary_store = primary_fungible_store::ensure_primary_store_exists(to, asset);
        fungible_asset::deposit_with_ref(transfer_ref, receiver_primary_store, fa);
    }

    /// Borrow the immutable reference of the refs of `metadata`.
    /// This validates that the signer is the metadata object's owner.
    inline fun authorized_borrow_refs(
        owner: &signer,
        asset: Object<Metadata>,
    ): &ManagingRefs acquires ManagingRefs {
        assert!(object::is_owner(asset, signer::address_of(owner)), error::permission_denied(ENOT_OWNER));
        borrow_global<ManagingRefs>(object::object_address(&asset))
    }

    #[test_only]
    use aptos_framework::account;

    #[test(creator = @0xcafe)]
    fun test_basic_flow(
        creator: &signer,
    ) acquires ManagingRefs {
        multisig_account::setup();
        let creator_address = signer::address_of(creator);
        account::create_account_for_test(creator_address);
        init_module(creator);
        let multisig_address = multisig_account::get_next_multisig_account_address(creator_address);
        let aaron_address = @0xface;

        let multisig_account = &account::create_signer_for_test(multisig_address);
        mint_to_primary_store(multisig_account, creator_address, 100);
        let asset = get_metadata();
        assert!(primary_fungible_store::balance(creator_address, asset) == 100, 4);
        set_primary_stores_frozen_status(multisig_account, vector[creator_address, aaron_address], true);
        assert!(primary_fungible_store::is_frozen(creator_address, asset), 5);
        assert!(primary_fungible_store::is_frozen(aaron_address, asset), 6);
        transfer_between_primary_stores(multisig_account, creator_address, aaron_address, 10);
        assert!(primary_fungible_store::balance(aaron_address, asset) == 10, 7);

        set_primary_stores_frozen_status(multisig_account, vector[creator_address, aaron_address], false);
        assert!(!primary_fungible_store::is_frozen(creator_address, asset), 8);
        assert!(!primary_fungible_store::is_frozen(aaron_address, asset), 9);
        burn_from_primary_store(multisig_account, creator_address, 90);
    }

    #[test(creator = @0xcafe, aaron = @0xface)]
    #[expected_failure(abort_code = 0x50001, location = Self)]
    fun test_permission_denied(
        creator: &signer,
        aaron: &signer
    ) acquires ManagingRefs {
        multisig_account::setup();
        let creator_address = signer::address_of(creator);
        account::create_account_for_test(creator_address);
        init_module(creator);
        let creator_address = signer::address_of(creator);
        mint_to_primary_store(aaron, creator_address, 100);
    }
}
