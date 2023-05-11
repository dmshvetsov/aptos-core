// Copyright © Aptos Foundation
// SPDX-License-Identifier: Apache-2.0

import { AptosAccount, HexString, Provider, Network, Types } from "aptos";

const provider = new Provider(Network.TESTNET);

async function getTokenAddr(ownerAddr: HexString, tokenName: string): Promise<HexString> {
    const tokenOwnership = await provider.getOwnedTokens(ownerAddr)
    for (const ownership of tokenOwnership.current_token_ownerships_v2) {
        if (ownership.current_token_data.token_name === tokenName) {
            return new HexString(ownership.current_token_data.token_data_id);
        }
    }
    console.log(`Token ${tokenName} not found`);
    process.exit(1);
}

async function waitForEnter() {
    return new Promise<void>((resolve, reject) => {
        const rl = require("readline").createInterface({
            input: process.stdin,
            output: process.stdout
        });

        rl.question('Please press the Enter key to proceed ...\n', () => {
            rl.close();
            resolve();
        });
    });
}

class AmbassadorClient {
    async setAmbassadorLevel(creator: AptosAccount, token: HexString, new_ambassador_level: number | bigint): Promise<string> {
        const rawTxn = await provider.generateTransaction(creator.address(), {
            function: `${creator.address()}::ambassador::set_ambassador_level`,
            type_arguments: [],
            arguments: [token.hex(), new_ambassador_level],
        });

        const bcsTxn = await provider.signTransaction(creator, rawTxn);
        const pendingTxn = await provider.submitTransaction(bcsTxn);

        return pendingTxn.hash;
    }

    async burn(creator: AptosAccount, token: HexString): Promise<string> {
        const rawTxn = await provider.generateTransaction(creator.address(), {
            function: `${creator.address()}::ambassador::burn`,
            type_arguments: [],
            arguments: [token.hex()],
        });

        const bcsTxn = await provider.signTransaction(creator, rawTxn);
        const pendingTxn = await provider.submitTransaction(bcsTxn);

        return pendingTxn.hash;
    }

    async mintAmbassadorToken(creator: AptosAccount, description: string, name: string, uri: string, soul_bound_to: HexString): Promise<string> {
        const rawTxn = await provider.generateTransaction(creator.address(), {
            function: `${creator.address()}::ambassador::mint_ambassador_token`,
            type_arguments: [],
            arguments: [description, name, uri, soul_bound_to.hex()],
        });

        const bcsTxn = await provider.signTransaction(creator, rawTxn);
        const pendingTxn = await provider.submitTransaction(bcsTxn);

        return pendingTxn.hash;
    }

    async ambassadorLevel(creator_addr: HexString, token_addr: HexString): Promise<bigint> {
        const payload: Types.ViewRequest = {
            function: `${creator_addr.hex()}::ambassador::ambassador_level`,
            type_arguments: [],
            arguments: [token_addr.hex()],
        };

        const result = await provider.view(payload);
        return BigInt(result[0] as any);
    }
}

/** run our demo! */
async function main(): Promise<void> {
    const client = new AmbassadorClient();

    // Set a test admin account
    const privateKeyBytes_admin = Uint8Array.from(Buffer.from('f21423f436f7d44c2abd95b5a25323e81fc737040ab17ae8fe40dbf1b1de9e66', 'hex'));
    const admin = new AptosAccount(privateKeyBytes_admin, '9bfdd4efe15f4d8aa145bef5f64588c7c391bcddaf34f9e977f59bd93b498f2a');
    // Set a test user account
    const userAddr = new HexString("4db1582c315ddd9f29db3dfcf0aa7f7467b1a4f2d1190bb93b8304cdc164490c");
    // Set a test token name
    const tokenName = "Aptos Ambassador #25";

    console.log("\n=== Addresses ===");
    console.log(`Admin: ${admin.address()}`);
    console.log(`User: ${userAddr}`);

    // Mint Ambassador Token
    let txnHash = await client.mintAmbassadorToken(admin, 'Aptos Ambassador Token', tokenName, 'https://raw.githubusercontent.com/aptos-labs/aptos-core/main/aptos-move/move-examples/token_objects/ambassador/metadata/ambassador_1/', userAddr);
    await provider.waitForTransaction(txnHash, { checkSuccess: true });
    console.log("\n=== Ambassador Token Minted ===");
    console.log(`Txn: https://explorer.aptoslabs.com/txn/${txnHash}?network=testnet`);
    // Get the address of the minted token
    const tokenAddr = await getTokenAddr(userAddr, tokenName);
    console.log(`The address of the minted token: ${tokenAddr}`);
    console.log(`The level of the token: ${await client.ambassadorLevel(admin.address(), tokenAddr)}`);
    await waitForEnter();

    // Set Ambassador Level to 15
    txnHash = await client.setAmbassadorLevel(admin, tokenAddr, 15);
    await provider.waitForTransaction(txnHash, { checkSuccess: true });
    console.log("\n=== Level set to 15 ===");
    console.log(`Txn: https://explorer.aptoslabs.com/txn/${txnHash}?network=testnet`);
    console.log(`The level of the token: ${await client.ambassadorLevel(admin.address(), tokenAddr)}`);
    await waitForEnter();

    // Set Ambassador Level to 25
    txnHash = await client.setAmbassadorLevel(admin, tokenAddr, 25);
    await provider.waitForTransaction(txnHash, { checkSuccess: true });
    console.log("\n=== Level set to 25 ===");
    console.log(`Txn: https://explorer.aptoslabs.com/txn/${txnHash}?network=testnet`);
    console.log(`The level of the token: ${await client.ambassadorLevel(admin.address(), tokenAddr)}`);
    await waitForEnter();

    // Burn the token
    txnHash = await client.burn(admin, tokenAddr);
    await provider.waitForTransaction(txnHash, { checkSuccess: true });
    console.log("\n=== Token burned ===");
    console.log(`Txn: https://explorer.aptoslabs.com/txn/${txnHash}?network=testnet`);
    await waitForEnter();
}

main().then(() => {
    console.log("Done!");
    process.exit(0);
});
