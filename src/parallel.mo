import Principal "mo:base/Principal";
import E "mo:candb/Entity";
import Iter "mo:base/Iter";
import Array "mo:base/Array";
import Buffer "mo:base/Buffer";
import Debug "mo:base/Debug";
import CanDB "mo:candb/CanDB";
import CanisterMap "mo:candb/CanisterMap";
import RBT "mo:stable-rbtree/StableRBTree";
import StableBuffer "mo:stablebuffer/StableBuffer";

module {
    type Result = {
        value: E.Entity;
        status: { #oneResult; #severalResults };
    };

    public func getAll(db: CanDB.DB, map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* RBT.Tree<Principal, E.Entity> {
        var result = RBT.init<Principal, E.Entity>();
        let canisters = CanisterMap.get(map, pk);
        let ?canisters2 = canisters else {
            return result;
        };
        let threads : [var ?(async ?E.Entity)] = Array.init(StableBuffer.size(canisters2), null);
        for (threadNum in threads.keys()) {
            let canister = StableBuffer.get(canisters2, threadNum);
            let partition = actor (canister) : actor { get : (options: CanDB.GetOptions) -> async ?E.Entity };
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
    public func getFirst(db: CanDB.DB, map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* ?(Principal, E.Entity) {
        let all = await* getAll(db, map, pk, options);
        RBT.entries(all).next();
    };

    public func getOne(db: CanDB.DB, map: CanisterMap.CanisterMap, pk: Text, options: CanDB.GetOptions) : async* ?(Principal, Result) {
        let all = await* getAll(db, map, pk, options);
        var iter = RBT.entries(all);
        let v = iter.next();
        switch (v) {
            case (?v) {
                ?(v.0, { value = v.1; status = if (iter.next() == null) { #oneResult } else { #severalResults }});
            };
            case null {
                null;
            };
        };
    };
}