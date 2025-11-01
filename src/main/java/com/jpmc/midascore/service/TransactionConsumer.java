package com.jpmc.midascore.service;

import com.jpmc.midascore.entity.TransactionRecord;
import com.jpmc.midascore.entity.UserRecord;
import com.jpmc.midascore.foundation.Transaction;
import com.jpmc.midascore.repository.TransactionRepository;
import com.jpmc.midascore.repository.UserRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class TransactionConsumer {

    private static final Logger logger = LoggerFactory.getLogger(TransactionConsumer.class);

    private final UserRepository userRepository;
    private final TransactionRepository transactionRepository;

    public TransactionConsumer(UserRepository userRepository, TransactionRepository transactionRepository) {
        this.userRepository = userRepository;
        this.transactionRepository = transactionRepository;
    }

    @KafkaListener(topics = "${general.kafka-topic}", groupId = "midas-group")
    @Transactional
    public void listen(Transaction transaction) {
        logger.info("Received transaction: {}", transaction);

        // Validate and process transaction
        UserRecord sender = userRepository.findById(transaction.getSenderId());
        UserRecord recipient = userRepository.findById(transaction.getRecipientId());

        // Check if sender and recipient exist
        if (sender == null || recipient == null) {
            logger.warn("Invalid transaction: sender or recipient not found");
            return;
        }

        // Check if sender has sufficient balance
        if (sender.getBalance() < transaction.getAmount()) {
            logger.warn("Invalid transaction: insufficient balance");
            return;
        }

        // Process valid transaction
        sender.setBalance(sender.getBalance() - transaction.getAmount());
        recipient.setBalance(recipient.getBalance() + transaction.getAmount());

        // Save updated balances
        userRepository.save(sender);
        userRepository.save(recipient);

        // Save transaction record
        TransactionRecord record = new TransactionRecord(sender, recipient, transaction.getAmount());
        transactionRepository.save(record);
        logger.info("Transaction processed successfully: {} -> {}, amount: {}",
                sender.getName(), recipient.getName(), transaction.getAmount());

        // Find balance of waldorf
        UserRecord waldorf = userRepository.findByName("waldorf");
        if (waldorf != null) {
            logger.info("Waldorf's balance: {}", waldorf.getBalance());
        }
    }
}