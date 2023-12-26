import Principal "mo:base/Principal";
import E "mo:candb/Entity";

module {
    type Result = {
        #read : {
            value: E.AttributeValue;
            status: { #oneResult; #severalResults };
        };
        #zeroResults;
    };

    public func getAll(db : DB, options : GetOptions) : RBT.Tree<Principal, E.Entity> {

    }

}