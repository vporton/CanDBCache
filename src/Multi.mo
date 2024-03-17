/// This is my extension to CanDB.
///
/// It supports:
///
/// * retrieving by key from multiple canisters
/// * storing ensuring that there are no duplicate keys in a partition.
///
/// This is useful for such tasks, as anti-Sybil protection, to ensure that there are no objects with a duplicate key.

import Principal "mo:base/Principal";
import E "mo:candb/Entity";
import Array "mo:base/Array";
import Debug "mo:base/Debug";
import Int "mo:base/Int";
import Text "mo:base/Text";
import CanDB "mo:candb/CanDB";
import CanisterMap "mo:candb/CanisterMap";
import RBT "mo:stable-rbtree/StableRBTree";
import StableBuffer "mo:stablebuffer/StableBuffer";

module {
    /// Get values from a DB with a given key for all canisters in `pk`.
    public func getAll(map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* RBT.Tree<Principal, E.Entity> {
        var result = RBT.init<Principal, E.Entity>();
        let canisters = CanisterMap.get(map, pk);
        let ?canisters2 = canisters else {
            return result;
        };
        let threads : [var ?(async ?E.Entity)] = Array.init(StableBuffer.size(canisters2), null);
        for (threadNum in threads.keys()) {
            let canister = StableBuffer.get(canisters2, threadNum);
            let partition = actor(canister) : actor { get : (options: CanDB.GetOptions) -> async ?E.Entity };
            threads[threadNum] := ?(partition.get(options)); // `??value`
        };
        for (tkey in threads.keys()) {
            let topt = threads[tkey];
            let ?t = topt else {
                Debug.trap("programming error: threads");
            };
            let aResult = await t;
            switch (aResult) {
                case (?v) {
                    let canister = StableBuffer.get(canisters2, tkey);
                    result := RBT.put<Principal, E.Entity>(result, Principal.compare, Principal.fromText(canister), v);
                };
                case null {};
            }
        };
        result;
    };

    /// Get the first value from a DB with a given key for all canisters in `pk`.
    public func getFirst(map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* ?(Principal, E.Entity) {
        let all = await* getAll(map, pk, options);
        RBT.entries(all).next();
    };

    /// Get the first attribute value from a DB with a given key for all canisters in `pk`.
    public func getFirstAttribute(
        map: CanisterMap.CanisterMap,
        pk: Text,
        options: { sk: E.SK; subkey: E.AttributeKey }
    ) : async* ?(Principal, ?E.AttributeValue) {
        let first = await* getFirst(map, pk, options);
        switch (first) {
            case (?(part, value)) {
                ?(part, RBT.get(value.attributes, Text.compare, options.subkey));
            };
            case null { null };
        };
    };

    /// Found just one result or several results?
    public type ResultStatus = { #oneResult; #severalResults };

    /// Get the first attribute value from a DB with a given key for all canisters in `pk` and
    /// return whether there were the same key in other canisters.
    public func getOne(map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* ?(Principal, E.Entity, ResultStatus) {
        let all = await* getAll(map, pk, options);
        var iter = RBT.entries(all);
        let v = iter.next();
        switch (v) {
            case (?v) {
                ?(v.0, v.1, if (iter.next() == null) { #oneResult } else { #severalResults });
            };
            case null {
                null;
            };
        };
    };

    /// Get the value from a canister specified by the `hint`. (`hint` is used to speedup lookup.)
    /// If there are no `hint`, return the value from the first canister.
    public func getByHint(map: CanisterMap.CanisterMap, pk: Text, hint: ?Principal, options: CanDB.GetOptions)
        : async* ?(Principal, E.Entity)
    {
        switch (hint) {
            case (?hint) {
                let part: actor {
                    get(options: CanDB.GetOptions): async ?E.Entity;
                } = actor(Principal.toText(hint));
                let res = await part.get(options);
                do ? { (hint, res!) };
            };
            case null {
                await* getFirst(map, pk, options);
            };
        }
    };

    /// Get the attribute from a canister specified by the `hint`. (`hint` is used to speedup lookup.)
    /// If there are no `hint`, return the value from the first canister.
    public func getAttributeByHint(
        map: CanisterMap.CanisterMap,
        pk: Text,
        hint: ?Principal,
        options: { sk: E.SK; subkey: E.AttributeKey }
    )
        : async* ?(Principal, ?E.AttributeValue)
    {
        switch (hint) {
            case (?hint) {
                let part: actor {
                    getAttribute(options: { sk: E.SK; subkey: E.AttributeKey }): async ?E.AttributeValue;
                } = actor(Principal.toText(hint));
                let res = await part.getAttribute(options);
                ?(hint, res);
            };
            case null {
                await* getFirstAttribute(map, pk, options);
            };
        }
    };

    // TODO: `getOneAttribute`

    // TODO: `has` counterparts of `get` methods

    // TODO: below race conditions

    /// Updates a value in the DB, only if the value specified by the key already exists.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func updateExisting(db: CanDB.DB, options: CanDB.UpdateOptions) : async* ?E.Entity {
        if (CanDB.skExists(db, options.sk)) {
            CanDB.update(db, options);
        } else {
            null;
        };
    };

    /// Updates an attribute in the DB, only if the value specified by the key already exists.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func updateExistingOrTrap(db: CanDB.DB, options: CanDB.UpdateOptions) : async* E.Entity {
        let ?entity = await* updateExisting(db, options) else {
            Debug.trap("no existing value");
        };
        entity;
    };

    /// Replaces (or creates) an attribute in the DB.
    public func replaceAttribute(db: CanDB.DB, options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue })
        : async* ?E.Entity
    {
        CanDB.update(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?E.AttributeMap): E.AttributeMap {
            let map = switch (old) {
                case (?old) { old };
                case null { RBT.init() };
            };
            RBT.put(map, Text.compare, options.subkey, options.value);
        }});
    };

    /// Replaces (or creates) an existing value in the DB.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExisting(db: CanDB.DB, options: CanDB.PutOptions) : async* ?E.Entity {
        var map = RBT.init<E.AttributeKey, E.AttributeValue>();
        for (e in options.attributes.vals()) {
            map := RBT.put(map, Text.compare, e.0, e.1);
        };
        await* updateExisting(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?E.AttributeMap): E.AttributeMap {
            map;
        }})
    };

    /// Replaces an existing attribute in the DB.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExistingAttribute(db: CanDB.DB, options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue })
        : async* ?E.Entity
    {
        await* updateExisting(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?E.AttributeMap): E.AttributeMap {
            let map = switch (old) {
                case (?old) { old };
                case null { RBT.init() };
            };
            RBT.put(map, Text.compare, options.subkey, options.value);
        }});
    };

    /// Replaces an existing attribute in the DB.
    /// If there is none, traps.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExistingOrTrap(db: CanDB.DB, options: CanDB.PutOptions) : async* E.Entity {
        let ?entity = await* replaceExisting(db, options) else {
            Debug.trap("no existing value");
        };
        entity;
    };

    /// Replaces an existing attribute in the DB.
    /// If there is none, traps.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExistingAttributeOrTrap(db: CanDB.DB, options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue })
        : async* E.Entity
    {
        let ?entity = await* replaceExistingAttribute(db, options) else {
            Debug.trap("no existing value");
        };
        entity;
    };

    /// Replaces an existing value in the DB.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putExisting(db: CanDB.DB, options: CanDB.PutOptions) : async* Bool {
        (await* replaceExisting(db, options)) != null;
    };

    /// Replaces an existing attribute in the DB.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putExistingAttribute(db: CanDB.DB, options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue })
        : async* Bool
    {
        (await* replaceExistingAttribute(db, options)) != null;
    };

    /// Replaces an existing value in the DB.
    /// If there are none, traps.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putExistingOrTrap(db: CanDB.DB, options: CanDB.PutOptions) : async* () {
        if (not(await* putExisting(db, options))) {
            Debug.trap("no existing value");
        }
    };

    /// Replaces an value in the DB or adds new value.
    ///
    /// This function may create duplicate values.
    public func putWithPossibleDuplicate(map: CanisterMap.CanisterMap, pk: Text, options: CanDB.PutOptions) : async* Principal {
        let canisters = CanisterMap.get(map, pk);
        let ?canisters2 = canisters else {
            Debug.trap("no partition canisters");
        };
        let canister = StableBuffer.get(canisters2, Int.abs(StableBuffer.size(canisters2) - 1));
        let partition = actor(canister) : actor { put : (options: CanDB.PutOptions) -> async () };
        await partition.put(options);
        Principal.fromText(canister);
    };

    /// Replaces an attribute in the DB or adds a new attribute.
    ///
    /// This function may create duplicate values.
    public func putAttribute(
        db: CanDB.DB,
        options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue }
    ) : async* () {
        ignore await* replaceAttribute(db, options);
    };

    /// Replaces an attribute in the DB or adds a new attribute.
    /// Return the canister to which it was added.
    ///
    /// This function may create duplicate values.
    public func putAttributeWithPossibleDuplicate(
        map: CanisterMap.CanisterMap,
        pk: Text,
        options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue }
    ) : async* Principal {
        let canisters = CanisterMap.get(map, pk);
        let ?canisters2 = canisters else {
            Debug.trap("no partition canisters");
        };
        let canister = StableBuffer.get(canisters2, Int.abs(StableBuffer.size(canisters2) - 1));
        let partition = actor(canister) : actor {
            putAttribute : (options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue }) -> async ();
        };
        await partition.putAttribute(options);
        Principal.fromText(canister);
    };

    public type PutNoDuplicatesIndex = actor { putExisting : (options: CanDB.PutOptions) -> async Bool; };

    /// Replaces an existing value in the DB or creates a new one.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putNoDuplicates(map: CanisterMap.CanisterMap, pk: Text, hint: ?Principal, options: CanDB.PutOptions)
        : async* Principal
    {
        // Duplicate code
        let first = await* getByHint(map, pk, hint, options);
        switch (first) {
            case (?(canister, entity)) {
                let partition = actor(Principal.toText(canister)) : actor {
                    put : (options: { sk: E.SK; attributes: [(E.AttributeKey, E.AttributeValue)] }) -> async ();
                };
                await partition.put(options);
                canister;
            };
            case null {
                await* putWithPossibleDuplicate(map, pk, options);
            };
        };
    };

    public type PutAttributeNoDuplicatesIndex = actor {
        putExistingAttribute : (options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue }) -> async Bool;
    };

    /// Replaces an existing attribute in the DB or creates a new one.
    ///
    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putAttributeNoDuplicates(
        map: CanisterMap.CanisterMap,
        pk: Text,
        hint: ?Principal,
        options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue }
    ) : async* Principal {
        // Duplicate code
        let first = await* getByHint(map, pk, hint, options);
        switch (first) {
            case (?(canister, entity)) {
                let partition = actor(Principal.toText(canister)) : actor {
                    putAttribute : (options: { sk: E.SK; subkey: E.AttributeKey; value: E.AttributeValue }) -> async ();
                };
                await partition.putAttribute(options);
                canister;
            };
            case null {
                await* putAttributeWithPossibleDuplicate(map, pk, options);
            };
        };
    };
}