#pragma once

#include <yql/essentials/sql/v1/complete/core/name.h>
#include <yql/essentials/sql/v1/complete/core/statement.h>

#include <library/cpp/threading/future/core/future.h>

#include <util/generic/vector.h>
#include <util/generic/string.h>
#include <util/generic/maybe.h>
#include <util/generic/hash_set.h>

namespace NSQLComplete {

    // TODO(YQL-19747): Rename to Identifier
    struct TIndentifier {
        TString Indentifier;
    };

    struct TNamespaced {
        TString Namespace;
    };

    struct TKeyword {
        TString Content;
    };

    struct TPragmaName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct TTypeName: TIndentifier {
        struct TConstraints {};

        enum class EKind {
            Simple,
            Container,
            Parameterized,
        };

        EKind Kind = EKind::Simple;
    };

    struct TFunctionName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct THintName: TIndentifier {
        struct TConstraints {
            EStatementKind Statement;
        };
    };

    struct TObjectNameConstraints {
        TString Provider;
        TString Cluster;
        THashSet<EObjectKind> Kinds;
    };

    struct TFolderName: TIndentifier {
    };

    struct TTableName: TIndentifier {
    };

    struct TClusterName: TIndentifier {
        struct TConstraints: TNamespaced {};
    };

    struct TColumnName: TIndentifier {
        struct TConstraints {
            TVector<TAliased<TTableId>> Tables;
        };

        TString TableAlias;
    };

    struct TBindingName: TIndentifier {
    };

    struct TUnkownName {
        TString Content;
        TString Type;
    };

    using TGenericName = std::variant<
        TKeyword,
        TPragmaName,
        TTypeName,
        TFunctionName,
        THintName,
        TFolderName,
        TTableName,
        TClusterName,
        TColumnName,
        TBindingName,
        TUnkownName>;

    struct TNameConstraints {
        TMaybe<TPragmaName::TConstraints> Pragma;
        TMaybe<TTypeName::TConstraints> Type;
        TMaybe<TFunctionName::TConstraints> Function;
        TMaybe<THintName::TConstraints> Hint;
        TMaybe<TObjectNameConstraints> Object;
        TMaybe<TClusterName::TConstraints> Cluster;
        TMaybe<TColumnName::TConstraints> Column;

        bool IsEmpty() const {
            return !Pragma &&
                   !Type &&
                   !Function &&
                   !Hint &&
                   !Object &&
                   !Cluster &&
                   !Column;
        }

        TGenericName Qualified(TGenericName unqualified) const;
        TGenericName Unqualified(TGenericName qualified) const;
        TVector<TGenericName> Qualified(TVector<TGenericName> unqualified) const;
        TVector<TGenericName> Unqualified(TVector<TGenericName> qualified) const;
    };

    struct TNameRequest {
        TVector<TString> Keywords;
        TNameConstraints Constraints;
        TString Prefix = "";
        size_t Limit = 128;

        bool IsEmpty() const {
            return Keywords.empty() && Constraints.IsEmpty();
        }
    };

    struct TNameResponse {
        TVector<TGenericName> RankedNames;
        TMaybe<size_t> NameHintLength = Nothing();

        bool IsEmpty() const {
            return RankedNames.empty();
        }
    };

    class INameService: public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<INameService>;

        ~INameService() override = default;
        virtual NThreading::TFuture<TNameResponse> Lookup(TNameRequest request) const = 0;
    };

    TString NormalizeName(TStringBuf name);

} // namespace NSQLComplete
