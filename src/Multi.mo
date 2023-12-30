import Principal "mo:base/Principal";
import E "mo:candb/Entity";
import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Int "mo:base/Int";
import Text "mo:base/Text";
import CanDB "mo:candb/CanDB";
import CanisterMap "mo:candb/CanisterMap";
import RBT "mo:stable-rbtree/StableRBTree";
import StableBuffer "mo:stablebuffer/StableBuffer";

module {
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

    // Can be made faster at expense of code size.
    public func getFirst(map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* ?(Principal, E.Entity) {
        let all = await* getAll(map, pk, options);
        RBT.entries(all).next();
    };

    public type ResultStatus = { #oneResult; #severalResults };

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

    // TODO: `has` counterparts of `get` methods

    // TODO: below race conditions

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func updateExisting(db: CanDB.DB, options: CanDB.UpdateOptions) : async* ?E.Entity {
        if (CanDB.skExists(db, options.sk)) {
            CanDB.update(db, options);
        } else {
            null;
        };
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func updateExistingOrTrap(db: CanDB.DB, options: CanDB.UpdateOptions) : async* E.Entity {
        let ?entity = await* updateExisting(db, options) else {
            Debug.trap("no existing value");
        };
        entity;
    };

    public func replaceAttribute(db: CanDB.DB, options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue })
        : async* ?E.Entity
    {
        CanDB.update(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?E.AttributeMap): E.AttributeMap {
            let map = switch (old) {
                case (?old) { old };
                case null { RBT.init() };
            };
            RBT.put(map, Text.compare, options.key, options.value);
        }});
    };

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

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExistingAttribute(db: CanDB.DB, options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue })
        : async* ?E.Entity
    {
        await* updateExisting(db, { sk = options.sk; updateAttributeMapFunction = func(old: ?E.AttributeMap): E.AttributeMap {
            let map = switch (old) {
                case (?old) { old };
                case null { RBT.init() };
            };
            RBT.put(map, Text.compare, options.key, options.value);
        }});
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExistingOrTrap(db: CanDB.DB, options: CanDB.PutOptions) : async* E.Entity {
        let ?entity = await* replaceExisting(db, options) else {
            Debug.trap("no existing value");
        };
        entity;
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func replaceExistingAttributeOrTrap(db: CanDB.DB, options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue })
        : async* E.Entity
    {
        let ?entity = await* replaceExistingAttribute(db, options) else {
            Debug.trap("no existing value");
        };
        entity;
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putExisting(db: CanDB.DB, options: CanDB.PutOptions) : async* Bool {
        (await* replaceExisting(db, options)) != null;
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putExistingAttribute(db: CanDB.DB, options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue })
        : async* Bool
    {
        (await* replaceExistingAttribute(db, options)) != null;
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func putExistingOrTrap(db: CanDB.DB, options: CanDB.PutOptions) : async* () {
        if (not(await* putExisting(db, options))) {
            Debug.trap("no existing value");
        }
    };

    public func putWithPossibleDuplicate(map: CanisterMap.CanisterMap, pk: Text, options: CanDB.PutOptions) : async* () {
        let canisters = CanisterMap.get(map, pk);
        let ?canisters2 = canisters else {
            Debug.trap("no partition canisters");
        };
        let canister = StableBuffer.get(canisters2, Int.abs(StableBuffer.size(canisters2) - 1));
        let partition = actor(canister) : actor { put : (options: CanDB.PutOptions) -> async () };
        await partition.put(options);
    };

    public func putAttribute(
        db: CanDB.DB,
        options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue }
    ) : async* () {
        ignore await* replaceAttribute(db, options);
    };

    public func putAttributeWithPossibleDuplicate(
        map: CanisterMap.CanisterMap,
        pk: Text,
        options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue }
    ) : async* () {
        let canisters = CanisterMap.get(map, pk);
        let ?canisters2 = canisters else {
            Debug.trap("no partition canisters");
        };
        let canister = StableBuffer.get(canisters2, Int.abs(StableBuffer.size(canisters2) - 1));
        let partition = actor(canister) : actor {
            putAttribute : (options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue }) -> async ();
        };
        await partition.putAttribute(options);
    };

    public type PutNoDuplicatesIndex = actor { putExisting : (options: CanDB.PutOptions) -> async Bool; };

    /// Ensures no duplicate SKs.
    public func putNoDuplicates(index: PutNoDuplicatesIndex, map: CanisterMap.CanisterMap, pk: Text, options: CanDB.PutOptions) : async* () {
        if (not(await index.putExisting(options))) {
            await* putWithPossibleDuplicate(map, pk, options);
        };
    };

    public type PutAttributeNoDuplicatesIndex = actor {
        putExistingAttribute : (options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue }) -> async Bool;
    };

    /// Ensures no duplicate SKs.
    public func putAttributeNoDuplicates(
        index: PutAttributeNoDuplicatesIndex,
        map: CanisterMap.CanisterMap,
        pk: Text,
        options: { sk: E.SK; key: E.AttributeKey; value: E.AttributeValue }
    ) : async* () {
        if (not(await index.putExistingAttribute(options))) {
            await* putAttributeWithPossibleDuplicate(map, pk, options);
        };
    };
}