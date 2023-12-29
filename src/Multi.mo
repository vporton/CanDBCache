import Principal "mo:base/Principal";
import E "mo:candb/Entity";
import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import Int "mo:base/Int";
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
    public func checkedReplace(db: CanDB.DB, options: CanDB.PutOptions) : async* Bool {
        let old = CanDB.get(db, options);
        switch (old) {
            case (?old) {
                await* CanDB.put(db, options);
                true;
            };
            case null {
                false;
            };
        };
    };

    /// This function is intended to ensure that a new value with the same SK is not introduced.
    public func checkedReplaceOrTrap(db: CanDB.DB, options: CanDB.PutOptions) : async* () {
        if (not(await* checkedReplace(db, options))) {
            Debug.trap("no value to replace");
        }
    };

    type PutNoDuplicatesIndex = actor { checkedReplace : (options: CanDB.PutOptions) -> async Bool; };

    /// Ensures no duplicate SKs.
    public func putNoDuplicates(index: PutNoDuplicatesIndex, map: CanisterMap.CanisterMap, pk: Text, options: CanDB.PutOptions) : async* () {
        if (not(await index.checkedReplace(options))) {
            let canisters = CanisterMap.get(map, pk);
            let ?canisters2 = canisters else {
                Debug.trap("no partition canisters");
            };
            let canister = StableBuffer.get(canisters2, Int.abs(StableBuffer.size(canisters2) - 1));
            let partition = actor(canister) : actor { put : (options: CanDB.PutOptions) -> async () };
            await partition.put(options);
        };
    };
}