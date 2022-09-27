// Copyright (c) Aptos
// SPDX-License-Identifier: Apache-2.0

import React from 'react';
import {
  Box,
  Button,
  Circle,
  Flex,
  HStack,
  Modal,
  ModalBody,
  ModalCloseButton,
  ModalContent,
  ModalFooter,
  ModalHeader,
  ModalOverlay,
  ModalProps,
  Spinner,
  Text,
  useColorMode,
  useDisclosure,
  useRadio,
  UseRadioProps,
  VStack,
} from '@chakra-ui/react';
import {
  customColors,
  secondaryHoverBgColor,
  secondaryButtonBgColor,
  secondaryDisabledNetworkBgColor,
  checkCircleSuccessBg,
  checkedBgColor,
} from 'core/colors';
import { AiFillCheckCircle } from '@react-icons/all-files/ai/AiFillCheckCircle';
import { DeleteIcon } from '@chakra-ui/icons';
import { useNodeStatus } from 'core/queries/network';
import { defaultNetworks, Network } from 'shared/types';

type ConfirmationModalProps = Omit<ModalProps, 'children'> & {
  name: string,
  onConfirm: () => void,
};

function ConfirmationModal(props: ConfirmationModalProps) {
  const { name, onClose, onConfirm } = props;

  return (
    <Modal {...props}>
      <ModalOverlay />
      <ModalContent>
        <ModalHeader>
          Remove network
        </ModalHeader>
        <ModalCloseButton />
        <ModalBody>
          { 'Are you sure you want to remove ' }
          <Text fontWeight="bold" as="span">{ name }</Text>
          ?
        </ModalBody>
        <ModalFooter>
          <Button colorScheme="red" mr={3} onClick={onConfirm}>
            Yes
          </Button>
          <Button onClick={onClose}>
            Close
          </Button>
        </ModalFooter>
      </ModalContent>
    </Modal>
  );
}

type NetworkListItemProps = UseRadioProps & {
  network: Network,
  onRemove: (networkName: string) => void,
};

export default function NetworkListItem(props: NetworkListItemProps) {
  const {
    isChecked,
    network,
    onRemove,
  } = props;

  const queryIntervalMs = 5000;
  const queryOptions = {
    cacheTime: queryIntervalMs,
    refetchInterval: queryIntervalMs,
    staleTime: queryIntervalMs,
  };
  const {
    isLoading: isNodeStatusLoading,
    isNodeAvailable,
  } = useNodeStatus(network.nodeUrl, queryOptions);
  const isDisabled = !isNodeAvailable;

  const { getCheckboxProps, getInputProps } = useRadio({ ...props, isDisabled });
  const { isOpen, onClose, onOpen } = useDisclosure();
  const { colorMode } = useColorMode();

  const isCustomNetwork = !(network.name in defaultNetworks);

  const onDeleteClick = (e: React.MouseEvent) => {
    e.preventDefault();
    onOpen();
  };

  const enabledBoxProps = !isDisabled ? {
    _hover: {
      bg: isChecked
        ? 'navy.700'
        : secondaryHoverBgColor[colorMode],
    },
    cursor: 'pointer',
  } : {};

  return (
    <Box as="label">
      <input disabled={isDisabled} {...getInputProps()} />
      <Box
        {...getCheckboxProps()}
        {...enabledBoxProps}
        borderRadius="md"
        border="1px"
        bgColor={secondaryButtonBgColor[colorMode]}
        borderColor={customColors.navy[200]}
        _disabled={{
          bg: secondaryDisabledNetworkBgColor[colorMode],
          color: 'navy.300',
        }}
        _checked={{
          bg: checkedBgColor[colorMode],
        }}
        _focus={{
          boxShadow: 'outline',
        }}
        px={5}
        py={3}
      >
        <HStack w="100%" justifyContent="space-between" maxWidth="300px">
          <VStack alignItems="flex-start" overflow="hidden">
            <HStack minWidth="320px">
              <Circle bg={isNodeAvailable ? 'green.300' : 'red.400'} size={2} as="span" />
              <Text fontSize="md" fontWeight={600}>
                {network.name}
              </Text>
              {
                isNodeStatusLoading
                  ? <Spinner ml={2} size="xs" as="span" />
                  : null
              }
            </HStack>
            <Text
              fontSize="md"
              fontWeight={400}
              w="100%"
              whiteSpace="nowrap"
              overflow="hidden"
              textOverflow="ellipsis"
            >
              {network.nodeUrl}
            </Text>
          </VStack>
          <Flex alignItems="center" gap={2}>
            {isChecked && <AiFillCheckCircle size={32} color={checkCircleSuccessBg[colorMode]} />}
            {
            isCustomNetwork ? (
              <DeleteIcon
                fontSize="lg"
                cursor="pointer"
                _hover={{ color: 'red.400' }}
                onClick={onDeleteClick}
              />
            ) : null
          }
          </Flex>
        </HStack>
      </Box>
      <ConfirmationModal
        isOpen={isOpen}
        onClose={onClose}
        onConfirm={() => onRemove(network.name)}
        name={network.name}
      />
    </Box>
  );
}
